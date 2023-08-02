use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{stream::Fuse, FutureExt, Stream, StreamExt};
use reth_interfaces::Error as RethError;
use reth_payload_builder::{
    database::CachedReads, error::PayloadBuilderError, BuiltPayload, KeepPayloadJobAlive,
    PayloadBuilderAttributes, PayloadJob, PayloadJobGenerator,
};
use reth_primitives::{
    constants::{BEACON_NONCE, EMPTY_OMMER_ROOT},
    proofs, Block, BlockNumber, ChainSpec, Header, Receipt, SealedHeader, TransactionSigned, U256,
};
use reth_provider::{
    BlockReaderIdExt, CanonStateNotification, PostState, StateProvider, StateProviderFactory,
};
use reth_revm::{
    database::State,
    env::tx_env_with_recovered,
    executor::{
        commit_state_changes, increment_account_balance, post_block_withdrawals_balance_increments,
    },
    into_reth_log,
    revm::{
        db::CacheDB,
        primitives::{result::InvalidTransaction, Env, ResultAndState},
        EVM,
    },
};
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    task,
};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};
use tokio_util::time::DelayQueue;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BundleCompact(Vec<TransactionSigned>);

impl BundleCompact {
    /// returns whether `self` conflicts with `other` in the sense that both cannot be executed
    pub fn conflicts(&self, other: &Self) -> bool {
        let hashes = self
            .0
            .iter()
            .map(|tx| tx.hash_ref())
            .collect::<HashSet<_>>();
        let other_hashes = other
            .0
            .iter()
            .map(|tx| tx.hash_ref())
            .collect::<HashSet<_>>();
        !hashes.is_disjoint(&other_hashes)
    }
}

type BundleId = u64;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Bundle {
    pub id: BundleId,
    pub txs: Vec<TransactionSigned>,
    pub block_num: BlockNumber,
    pub eligibility: RangeInclusive<u64>,
}

#[derive(Default)]
pub struct BundlePool(HashSet<Bundle>);

impl BundlePool {
    /// returns all bundles eligible w.r.t. time `now` and canonical chain tip `block`
    pub fn eligible(&self, block: BlockNumber, now: SystemTime) -> Vec<Bundle> {
        let now = now.duration_since(UNIX_EPOCH).unwrap().as_secs();
        self.0
            .iter()
            .filter(|bundle| bundle.eligibility.contains(&now) && bundle.block_num == block)
            .cloned()
            .collect()
    }

    /// removes all bundles whose eligibility expires w.r.t. time `now`
    pub fn tick(&mut self, now: SystemTime) {
        let now = now.duration_since(UNIX_EPOCH).unwrap().as_secs();
        self.0.retain(|bundle| *bundle.eligibility.end() >= now);
    }

    /// maintains the pool based on updates to the canonical state
    pub fn maintain(&mut self, _event: CanonStateNotification) {
        // remove all bundles
        self.0.clear();
    }
}

struct Payload {
    inner: Arc<BuiltPayload>,
    bundles: HashSet<BundleId>,
}

#[derive(Clone, Debug)]
struct JobConfig {
    attributes: PayloadBuilderAttributes,
    parent: Arc<SealedHeader>,
    chain: Arc<ChainSpec>,
    extra_data: u128,
}

/// a build job scoped to `config`
pub struct Job<Client> {
    config: JobConfig,
    client: Arc<Client>,
    bundles: HashMap<BundleId, BundleCompact>,
    incoming: Fuse<BroadcastStream<(BundleId, BlockNumber, BundleCompact)>>,
    invalidated: Fuse<BroadcastStream<BundleId>>,
    built_payloads: Vec<Payload>,
    pending_payloads: VecDeque<task::JoinHandle<Result<Payload, PayloadBuilderError>>>,
}

impl<Client> Job<Client> {
    fn new<I: Iterator<Item = Bundle>>(
        config: JobConfig,
        client: Arc<Client>,
        bundles: I,
        incoming: Fuse<BroadcastStream<(BundleId, BlockNumber, BundleCompact)>>,
        invalidated: Fuse<BroadcastStream<BundleId>>,
    ) -> Self {
        let bundles = bundles
            .map(|bundle| (bundle.id, BundleCompact(bundle.txs)))
            .collect();
        let built_payloads = Vec::new();
        let pending_payloads = VecDeque::new();

        Self {
            config,
            client,
            bundles,
            invalidated,
            incoming,
            built_payloads,
            pending_payloads,
        }
    }
}

impl<Client> Future for Job<Client>
where
    Client: StateProviderFactory + 'static,
{
    type Output = Result<(), PayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let config = self.config.clone();

        let this = self.get_mut();

        // incorporate new incoming bundles
        let mut num_incoming_bundles = 0;
        let mut incoming = Pin::new(&mut this.incoming);
        loop {
            match incoming.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok((id, block_num, bundle)))) => {
                    // if the bundle is not eligible for the job, then skip the bundle
                    if block_num != this.config.parent.number + 1 {
                        continue;
                    }

                    this.bundles.insert(id, bundle);
                    num_incoming_bundles += 1;
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(_skipped)))) => continue,
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // remove any invalidated bundles
        let mut expired_bundles = HashSet::new();
        let mut invalidated = Pin::new(&mut this.invalidated);
        loop {
            match invalidated.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(exp))) => {
                    this.bundles.remove(&exp);
                    expired_bundles.insert(exp);
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(_skipped)))) => continue,
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // remove all payloads that contain an expired bundle
        this.built_payloads
            .retain(|payload| payload.bundles.is_disjoint(&expired_bundles));

        // if there are any expired or new bundles, then build a new payload
        if !expired_bundles.is_empty() || num_incoming_bundles > 0 {
            // NOTE: here we greedily select bundles that do not "obviously conflict" with
            // previously selected bundles. you could do far more sophisticated things here.
            let mut bundles: Vec<(BundleId, BundleCompact)> = vec![];
            for (id, bundle) in &this.bundles {
                if !bundles.iter().any(|(_, b)| b.conflicts(bundle)) {
                    bundles.push((*id, bundle.clone()));
                }
            }

            let client = Arc::clone(&this.client);
            let pending = task::spawn_blocking(move || {
                // TODO: come back to this
                build(config, client, CachedReads::default(), bundles.into_iter())
            });

            this.pending_payloads.push_back(pending);
        }

        // poll all pending payloads
        while let Some(mut pending) = this.pending_payloads.pop_front() {
            match pending.poll_unpin(cx) {
                Poll::Ready(payload) => {
                    match payload {
                        Ok(Ok(payload)) => {
                            // cache the built payload
                            this.built_payloads.push(payload);
                        }
                        Ok(Err(..)) => {
                            // build task failed
                        }
                        Err(..) => {
                            // `recv` failed
                        }
                    }
                }
                Poll::Pending => this.pending_payloads.push_back(pending),
            }
        }

        // keep payloads sorted
        this.built_payloads
            .sort_by_key(|payload| payload.inner.fees());

        Poll::Pending
    }
}

pub struct PayloadTask {
    best_payload: Option<Arc<BuiltPayload>>,
    empty_payload: Option<oneshot::Receiver<Result<Payload, PayloadBuilderError>>>,
}

impl Future for PayloadTask {
    type Output = Result<Arc<BuiltPayload>, PayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(best) = this.best_payload.take() {
            return Poll::Ready(Ok(best));
        }

        let mut empty = this.empty_payload.take().unwrap();
        match empty.poll_unpin(cx) {
            Poll::Ready(Ok(res)) => Poll::Ready(res.map(|p| p.inner)),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
            Poll::Pending => {
                this.empty_payload = Some(empty);
                Poll::Pending
            }
        }
    }
}

impl<Client> PayloadJob for Job<Client>
where
    Client: StateProviderFactory + Send + Sync + 'static,
{
    type ResolvePayloadFuture = PayloadTask;

    fn best_payload(&self) -> Result<Arc<BuiltPayload>, PayloadBuilderError> {
        if let Some(best) = self.built_payloads.first() {
            return Ok(Arc::clone(&best.inner));
        }

        let empty = build(
            self.config.clone(),
            Arc::clone(&self.client),
            CachedReads::default(),
            self.bundles.clone().into_iter(),
        )?;
        Ok(empty.inner)
    }

    fn resolve(&mut self) -> (Self::ResolvePayloadFuture, KeepPayloadJobAlive) {
        let best_payload = self.built_payloads.first().map(|p| p.inner.clone());

        // if there is no best payload, then build an empty payload
        let empty_payload = if best_payload.is_none() {
            let (tx, rx) = oneshot::channel();
            let config = self.config.clone();
            let client = Arc::clone(&self.client);
            let bundles = self.bundles.clone().into_iter();
            task::spawn_blocking(move || {
                let payload = build(config, client, CachedReads::default(), bundles);
                let _ = tx.send(payload);
            });

            Some(rx)
        } else {
            None
        };

        (
            PayloadTask {
                best_payload,
                empty_payload,
            },
            KeepPayloadJobAlive::No,
        )
    }
}

pub struct Builder<Client> {
    chain: Arc<ChainSpec>,
    client: Arc<Client>,
    extra_data: u128,
    bundle_pool: Arc<Mutex<BundlePool>>,
    incoming: broadcast::Sender<(BundleId, BlockNumber, BundleCompact)>,
    invalidated: broadcast::Sender<BundleId>,
}

impl<Client> Builder<Client>
where
    Client: StateProviderFactory + Unpin,
{
    pub fn new(chain: ChainSpec, extra_data: u128, client: Client) -> Self {
        let chain = Arc::new(chain);
        let client = Arc::new(client);
        let (incoming, _) = broadcast::channel(256);
        let (invalidated, _) = broadcast::channel(256);

        let bundle_pool = BundlePool::default();
        let bundle_pool = Arc::new(Mutex::new(bundle_pool));

        Self {
            chain,
            client,
            extra_data,
            bundle_pool,
            incoming,
            invalidated,
        }
    }

    /// spawns the builder maintenance task
    pub fn start(
        &self,
        mut bundle_flow: mpsc::UnboundedReceiver<Bundle>,
        mut state_events: mpsc::UnboundedReceiver<CanonStateNotification>,
    ) {
        let bundle_pool = Arc::clone(&self.bundle_pool);
        let invalidated = self.invalidated.clone();
        let incoming = self.incoming.clone();

        tokio::spawn(async move {
            // bundle refresh interval
            let mut interval = tokio::time::interval(Duration::from_secs(1));

            // track bundle expirations
            let mut bundle_expirations = DelayQueue::new();

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        bundle_pool.lock().unwrap().tick(SystemTime::now());
                    }
                    Some(bundle) = bundle_flow.recv() => {
                        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

                        // if the bundle already expired, then ignore it
                        if *bundle.eligibility.end() <= now {
                            continue;
                        }

                        // track the timeout of the bundle
                        let timeout = Duration::from_secs(bundle.eligibility.end() - now);
                        bundle_expirations.insert(bundle.id, timeout);

                        bundle_pool.lock().unwrap().0.insert(bundle.clone());

                        // notify jobs about new bundle
                        //
                        // NOTE: you could create metadata (e.g. access list) about the bundle here
                        // or within each job
                        let Bundle { id, txs, block_num, .. } = bundle;
                        let _ = incoming.send((id, block_num, BundleCompact(txs)));
                    }
                    Some(expired) = bundle_expirations.next() => {
                        // notify jobs about expired bundle
                        let _ = invalidated.send(expired.into_inner());
                    }
                    Some(event) = state_events.recv() => {
                        bundle_pool.lock().unwrap().maintain(event);
                    }
                }
            }
        });
    }
}

impl<Client> PayloadJobGenerator for Builder<Client>
where
    Client: StateProviderFactory + BlockReaderIdExt + 'static,
{
    type Job = Job<Client>;

    fn new_payload_job(
        &self,
        attributes: PayloadBuilderAttributes,
    ) -> Result<Self::Job, PayloadBuilderError> {
        // retrieve the latest block
        let latest = self
            .client
            .latest_header()?
            .ok_or_else(|| PayloadBuilderError::MissingParentBlock(attributes.parent))?;

        // only build on the latest block
        if attributes.parent != latest.hash() {
            return Err(PayloadBuilderError::Internal(RethError::Custom(
                "must build on latest block".into(),
            )));
        }

        let parent = Arc::new(latest.header.seal_slow());
        let config = JobConfig {
            chain: Arc::clone(&self.chain),
            parent,
            attributes,
            extra_data: self.extra_data,
        };

        // collect eligible bundles from the pool
        //
        // NOTE: it may make more sense to use `attributes.timestamp` here in call to `eligible`
        let bundles = self
            .bundle_pool
            .lock()
            .unwrap()
            .eligible(config.parent.number, SystemTime::now());

        let incoming = BroadcastStream::new(self.incoming.subscribe()).fuse();
        let invalidated = BroadcastStream::new(self.invalidated.subscribe()).fuse();

        Ok(Job::new(
            config,
            Arc::clone(&self.client),
            bundles.into_iter(),
            incoming,
            invalidated,
        ))
    }
}

fn build<Client: StateProviderFactory, I: Iterator<Item = (BundleId, BundleCompact)>>(
    config: JobConfig,
    client: Arc<Client>,
    cached_reads: CachedReads,
    bundles: I,
) -> Result<Payload, PayloadBuilderError> {
    let state = client.state_by_block_hash(config.parent.hash)?;
    let state = State::new(state);
    build_on_state(config, state, cached_reads, bundles)
}

fn build_on_state<S: StateProvider, I: Iterator<Item = (BundleId, BundleCompact)>>(
    config: JobConfig,
    state: State<S>,
    mut cached_reads: CachedReads,
    bundles: I,
) -> Result<Payload, PayloadBuilderError> {
    let mut db = CacheDB::new(cached_reads.as_db(&state));
    let mut post_state = PostState::default();

    let (cfg_env, block_env) = config
        .attributes
        .cfg_and_block_env(&config.chain, &config.parent);
    let base_fee = block_env.basefee.to::<u64>();
    let block_num = block_env.number.to::<u64>();
    let block_gas_limit: u64 = block_env.gas_limit.try_into().unwrap_or(u64::MAX);

    let mut total_fees = U256::ZERO;
    let mut cumulative_gas_used = 0;
    let mut txs = Vec::new();
    let mut bundle_ids = HashSet::new();

    for (id, bundle) in bundles {
        // check gas for entire bundle
        let bundle_gas_limit: u64 = bundle.0.iter().map(|tx| tx.gas_limit()).sum();
        if cumulative_gas_used + bundle_gas_limit > block_gas_limit {
            continue;
        }

        for tx in bundle.0.into_iter() {
            let tx =
                tx.into_ecrecovered()
                    .ok_or(PayloadBuilderError::Internal(RethError::Custom(
                        "unable to recover tx signer".into(),
                    )))?;

            // construct EVM
            let tx_env = tx_env_with_recovered(&tx);
            let env = Env {
                cfg: cfg_env.clone(),
                block: block_env.clone(),
                tx: tx_env.clone(),
            };
            let mut evm = EVM::with_env(env);
            evm.database(&mut db);

            // NOTE: you can do far more reasonable error handling here. if a transaction
            // within a bundle fails, we don't have to fail the entire payload.
            let ResultAndState { result, state } = evm
                .transact()
                .map_err(PayloadBuilderError::EvmExecutionError)?;

            commit_state_changes(&mut db, &mut post_state, block_num, state, true);

            post_state.add_receipt(
                block_num,
                Receipt {
                    tx_type: tx.tx_type(),
                    success: result.is_success(),
                    cumulative_gas_used,
                    logs: result.logs().into_iter().map(into_reth_log).collect(),
                },
            );

            cumulative_gas_used += result.gas_used();

            let miner_fee = tx
                .effective_tip_per_gas(base_fee)
                .ok_or(InvalidTransaction::GasPriceLessThanBasefee)
                .map_err(|err| PayloadBuilderError::EvmExecutionError(err.into()))?;
            total_fees += U256::from(miner_fee) * U256::from(result.gas_used());

            txs.push(tx.into_signed());
            bundle_ids.insert(id);
        }
    }

    // NOTE: here we assume post-shanghai
    let balance_increments = post_block_withdrawals_balance_increments(
        &config.chain,
        config.attributes.timestamp,
        &config.attributes.withdrawals,
    );
    for (address, increment) in balance_increments {
        increment_account_balance(&mut db, &mut post_state, block_num, address, increment)?;
    }
    let withdrawals_root = proofs::calculate_withdrawals_root(&config.attributes.withdrawals);

    // compute other accumulators
    let receipts_root = post_state.receipts_root(block_num);
    let logs_bloom = post_state.logs_bloom(block_num);
    let transactions_root = proofs::calculate_transaction_root(&txs);
    let state_root = state.state().state_root(post_state)?;

    let header = Header {
        parent_hash: config.parent.hash,
        ommers_hash: EMPTY_OMMER_ROOT,
        beneficiary: block_env.coinbase,
        state_root,
        transactions_root,
        receipts_root,
        withdrawals_root: Some(withdrawals_root),
        logs_bloom,
        timestamp: config.attributes.timestamp,
        mix_hash: config.attributes.prev_randao,
        nonce: BEACON_NONCE,
        base_fee_per_gas: Some(base_fee),
        number: config.parent.number + 1,
        gas_limit: block_gas_limit,
        difficulty: U256::ZERO,
        gas_used: cumulative_gas_used,
        extra_data: config.extra_data.to_le_bytes().into(),
    };

    let block = Block {
        header,
        body: txs,
        ommers: vec![],
        withdrawals: Some(config.attributes.withdrawals),
    };
    let block = block.seal_slow();

    let payload = BuiltPayload::new(config.attributes.id, block, total_fees);
    let payload = Payload {
        inner: Arc::new(payload),
        bundles: bundle_ids,
    };

    Ok(payload)
}
