use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::matches;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ethers::{
    signers::{LocalWallet, Signer},
    types::{
        transaction::{
            eip1559::Eip1559TransactionRequest, eip2718::TypedTransaction, eip2930::AccessList,
        },
        Bytes as EthersBytes, NameOrAddress,
    },
};
use futures_util::{stream::Fuse, FutureExt, Stream, StreamExt};
use reth_interfaces::Error as RethError;
use reth_payload_builder::{
    error::PayloadBuilderError, BuiltPayload, KeepPayloadJobAlive, PayloadBuilderAttributes,
    PayloadJob, PayloadJobGenerator,
};
use reth_primitives::{
    constants::{BEACON_NONCE, EMPTY_OMMER_ROOT},
    proofs, Block, BlockNumber, Bytes, ChainSpec, Header, IntoRecoveredTransaction, Receipt,
    SealedBlock, SealedHeader, TransactionSigned, TransactionSignedEcRecovered, U256,
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
        db::{CacheDB, DatabaseRef},
        primitives::{BlockEnv, CfgEnv, EVMError, Env, InvalidTransaction, ResultAndState, B160},
        EVM,
    },
};
use reth_transaction_pool::{noop::NoopTransactionPool, TransactionPool};
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    task,
    time::{sleep, Sleep},
};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};
use tokio_util::time::DelayQueue;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BundleCompact(Vec<TransactionSignedEcRecovered>);

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
    pub txs: Vec<TransactionSignedEcRecovered>,
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

    /// maintains the pool based on updates to the canonical state.
    ///
    /// returns the IDs of the bundles removed from the pool.
    pub fn maintain(&mut self, _event: CanonStateNotification) -> Vec<BundleId> {
        // remove all bundles
        self.0.drain().map(|bundle| bundle.id).collect()
    }
}

struct Payload {
    inner: Arc<BuiltPayload>,
    bundles: HashSet<BundleId>,
}

#[derive(Clone, Debug)]
struct PayloadAttributes {
    inner: PayloadBuilderAttributes,
    extra_data: u128,
    wallet: LocalWallet,
}

#[derive(Clone, Debug)]
struct JobConfig {
    attributes: PayloadAttributes,
    parent: Arc<SealedHeader>,
    chain: Arc<ChainSpec>,
}

/// a build job scoped to `config`
pub struct Job<Client, Pool> {
    config: JobConfig,
    deadline: Pin<Box<Sleep>>,
    client: Arc<Client>,
    pool: Arc<Pool>,
    bundles: HashMap<BundleId, BundleCompact>,
    incoming: Fuse<BroadcastStream<(BundleId, BlockNumber, BundleCompact)>>,
    invalidated: Fuse<BroadcastStream<BundleId>>,
    built_payloads: Vec<Payload>,
    pending_payloads: VecDeque<task::JoinHandle<Result<Payload, PayloadBuilderError>>>,
}

impl<Client, Pool> Job<Client, Pool> {
    fn new<I: IntoIterator<Item = Bundle>>(
        config: JobConfig,
        deadline: Pin<Box<Sleep>>,
        client: Arc<Client>,
        pool: Arc<Pool>,
        bundles: I,
        incoming: Fuse<BroadcastStream<(BundleId, BlockNumber, BundleCompact)>>,
        invalidated: Fuse<BroadcastStream<BundleId>>,
    ) -> Self {
        let bundles = bundles
            .into_iter()
            .map(|bundle| (bundle.id, BundleCompact(bundle.txs)))
            .collect();
        let built_payloads = Vec::new();
        let pending_payloads = VecDeque::new();

        Self {
            config,
            deadline,
            client,
            pool,
            bundles,
            invalidated,
            incoming,
            built_payloads,
            pending_payloads,
        }
    }
}

impl<Client, Pool> Future for Job<Client, Pool>
where
    Client: StateProviderFactory + 'static,
    Pool: TransactionPool + 'static,
{
    type Output = Result<(), PayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let config = self.config.clone();

        let this = self.get_mut();

        // check whether the deadline for the job expired
        if this.deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Ok(()));
        }

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
            let pool = Arc::clone(&this.pool);
            let pending = task::spawn_blocking(move || {
                // TODO: come back to this
                build(config, client, pool, bundles)
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

impl<Client, Pool> PayloadJob for Job<Client, Pool>
where
    Client: StateProviderFactory + Send + Sync + 'static,
    Pool: TransactionPool + 'static,
{
    type ResolvePayloadFuture = PayloadTask;

    fn best_payload(&self) -> Result<Arc<BuiltPayload>, PayloadBuilderError> {
        if let Some(best) = self.built_payloads.first() {
            return Ok(Arc::clone(&best.inner));
        }

        let empty = build(
            self.config.clone(),
            Arc::clone(&self.client),
            Arc::new(NoopTransactionPool::default()),
            None,
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
            let pool = Arc::new(NoopTransactionPool::default());
            task::spawn_blocking(move || {
                let payload = build(config, client, pool, None);
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

#[derive(Clone, Debug)]
pub struct BuilderConfig {
    pub deadline: Duration,
    pub extra_data: u128,
    pub wallet: LocalWallet,
}

pub struct Builder<Client, Pool> {
    chain: Arc<ChainSpec>,
    deadline: Duration,
    wallet: LocalWallet,
    extra_data: u128,
    client: Arc<Client>,
    pool: Arc<Pool>,
    bundle_pool: Arc<Mutex<BundlePool>>,
    incoming: broadcast::Sender<(BundleId, BlockNumber, BundleCompact)>,
    invalidated: broadcast::Sender<BundleId>,
}

impl<Client, Pool> Builder<Client, Pool>
where
    Client: StateProviderFactory + Unpin,
    Pool: TransactionPool + Unpin,
{
    pub fn new(config: BuilderConfig, chain: ChainSpec, client: Client, pool: Pool) -> Self {
        let chain = Arc::new(chain);
        let client = Arc::new(client);
        let pool = Arc::new(pool);
        let (incoming, _) = broadcast::channel(256);
        let (invalidated, _) = broadcast::channel(256);

        let bundle_pool = BundlePool::default();
        let bundle_pool = Arc::new(Mutex::new(bundle_pool));

        Self {
            chain,
            deadline: config.deadline,
            wallet: config.wallet,
            extra_data: config.extra_data,
            client,
            pool,
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
                        // maintain the bundle pool based on state events. notify jobs about
                        // invalidated bundles.
                        let removed = bundle_pool.lock().unwrap().maintain(event);
                        for bundle in removed {
                            let _ = invalidated.send(bundle);
                        }
                    }
                }
            }
        });
    }
}

impl<Client, Pool> PayloadJobGenerator for Builder<Client, Pool>
where
    Client: StateProviderFactory + BlockReaderIdExt + 'static,
    Pool: TransactionPool + 'static,
{
    type Job = Job<Client, Pool>;

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

        let attributes = PayloadAttributes {
            inner: attributes,
            extra_data: self.extra_data,
            wallet: self.wallet.clone(),
        };

        let parent = Arc::new(latest.header.seal_slow());
        let config = JobConfig {
            attributes,
            chain: Arc::clone(&self.chain),
            parent,
        };

        let deadline = Box::pin(sleep(self.deadline));

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
            deadline,
            Arc::clone(&self.client),
            Arc::clone(&self.pool),
            bundles,
            incoming,
            invalidated,
        ))
    }
}

fn build<Client, P, I>(
    config: JobConfig,
    client: Arc<Client>,
    pool: P,
    bundles: I,
) -> Result<Payload, PayloadBuilderError>
where
    Client: StateProviderFactory,
    P: TransactionPool,
    I: IntoIterator<Item = (BundleId, BundleCompact)>,
{
    let state = client.state_by_block_hash(config.parent.hash)?;
    let state = State::new(state);
    build_on_state(config, state, pool, bundles)
}

fn build_on_state<S, P, I>(
    config: JobConfig,
    state: State<S>,
    pool: P,
    bundles: I,
) -> Result<Payload, PayloadBuilderError>
where
    S: StateProvider,
    P: TransactionPool,
    I: IntoIterator<Item = (BundleId, BundleCompact)>,
{
    let state = Arc::new(state);
    let mut db = CacheDB::new(Arc::clone(&state));

    let mut post_state = PostState::default();

    let (cfg_env, mut block_env) = config
        .attributes
        .inner
        .cfg_and_block_env(&config.chain, &config.parent);

    // mark the builder as the coinbase in the block env
    block_env.coinbase = config.attributes.wallet.address().into();

    let block_num = block_env.number.to::<u64>();
    let base_fee = block_env.basefee.to::<u64>();
    let block_gas_limit: u64 = block_env.gas_limit.try_into().unwrap_or(u64::MAX);

    // reserve gas for end-of-block proposer payment
    const PROPOSER_PAYMENT_GAS_ALLOWANCE: u64 = 21000;
    let execution_gas_limit = block_gas_limit - PROPOSER_PAYMENT_GAS_ALLOWANCE;

    let mut coinbase_payment = U256::ZERO;
    let mut cumulative_gas_used = 0;
    let mut txs = Vec::new();
    let mut bundle_ids = HashSet::new();

    // execute bundles
    for (id, bundle) in bundles {
        // check gas for entire bundle
        let bundle_gas_limit: u64 = bundle.0.iter().map(|tx| tx.gas_limit()).sum();
        if cumulative_gas_used + bundle_gas_limit > execution_gas_limit {
            continue;
        }

        // clone the database, so that if the execution fails, then we can keep the state of the
        // database as if the execution was never attempted. currently, there is no way to roll
        // back the database state if the execution fails part-way through.
        //
        // NOTE: we will be able to refactor to do rollbacks after the following is merged:
        // https://github.com/paradigmxyz/reth/pull/3512
        let mut execution_db = db.clone();
        let mut execution_post_state = post_state.clone();

        let mut bundle = bundle.0;
        let execution = execute(
            &mut execution_db,
            &mut execution_post_state,
            &cfg_env,
            &block_env,
            cumulative_gas_used,
            bundle.clone(),
        );
        match execution {
            Ok(execution) => {
                coinbase_payment += execution.coinbase_payment;
                cumulative_gas_used = execution.cumulative_gas_used;
                txs.append(&mut bundle);

                db = execution_db;
                post_state = execution_post_state;
            }
            Err(_) => continue,
        }

        // add bundle to set of executed bundles
        bundle_ids.insert(id);
    }

    // execute transactions from mempool
    //
    // TODO: support more sophisticated mixtures of bundles and transactions
    let mut mempool_txs = pool.best_transactions_with_base_fee(base_fee);
    while let Some(tx) = mempool_txs.next() {
        // if we don't have sufficient gas for the transaction, then we skip past it. we also mark
        // the transaction invalid, which will remove any subsequent transactions that depend on it
        // from the iterator.
        if cumulative_gas_used + tx.gas_limit() > execution_gas_limit {
            mempool_txs.mark_invalid(&tx);
            continue;
        }

        let recovered_tx = tx.to_recovered_transaction();

        // NOTE: we do not need to clone the DB here as we do for bundle execution
        let execution = execute(
            &mut db,
            &mut post_state,
            &cfg_env,
            &block_env,
            cumulative_gas_used,
            Some(recovered_tx.clone()),
        );
        match execution {
            Ok(execution) => {
                coinbase_payment += execution.coinbase_payment;
                cumulative_gas_used = execution.cumulative_gas_used;
                txs.push(recovered_tx);
            }
            // if we have any transaction error other than the nonce being too low, then we mark
            // the transaction invalid
            Err(EVMError::Transaction(err)) => {
                if !matches!(err, InvalidTransaction::NonceTooLow { .. }) {
                    mempool_txs.mark_invalid(&tx);
                }
            }
            // treat any other errors as fatal
            Err(err) => return Err(PayloadBuilderError::EvmExecutionError(err)),
        }
    }

    // construct payment to proposer fee recipient.
    //
    // NOTE: we give the entire coinbase payment to the proposer, except for the gas that we need
    // to execute the transaction. if the coinbase payment cannot cover the gas cost to pay the
    // proposer, then we do not make any payment.
    let payment_tx_gas_cost = block_env.basefee * U256::from(PROPOSER_PAYMENT_GAS_ALLOWANCE);
    let proposer_payment = coinbase_payment.saturating_sub(payment_tx_gas_cost);
    if proposer_payment > U256::ZERO {
        let builder_acct = db
            .basic(block_env.coinbase)?
            .expect("builder account exists if coinbase payment non-zero");
        let payment_tx = proposer_payment_tx(
            &config.attributes.wallet,
            builder_acct.nonce,
            base_fee,
            cfg_env.chain_id.to::<u64>(),
            &config.attributes.inner.suggested_fee_recipient,
            proposer_payment,
        );

        // execute payment to proposer fee recipient
        //
        // if the payment transaction fails, then the entire payload build fails
        let execution = execute(
            &mut db,
            &mut post_state,
            &cfg_env,
            &block_env,
            cumulative_gas_used,
            Some(payment_tx.clone()),
        )
        .map_err(PayloadBuilderError::EvmExecutionError)?;
        cumulative_gas_used = execution.cumulative_gas_used;
        txs.push(payment_tx);
    }

    // NOTE: here we assume post-shanghai
    let balance_increments = post_block_withdrawals_balance_increments(
        &config.chain,
        config.attributes.inner.timestamp,
        &config.attributes.inner.withdrawals,
    );
    for (address, increment) in balance_increments {
        increment_account_balance(&mut db, &mut post_state, block_num, address, increment)?;
    }

    let block = package_block(
        state.state(),
        &config.attributes.inner,
        config.attributes.extra_data,
        &block_env,
        txs.into_iter().map(|tx| tx.into_signed()).collect(),
        post_state,
        cumulative_gas_used,
    )?;

    let payload = BuiltPayload::new(config.attributes.inner.id, block, proposer_payment);
    let payload = Payload {
        inner: Arc::new(payload),
        bundles: bundle_ids,
    };

    Ok(payload)
}

#[derive(Clone, Debug)]
struct Execution {
    cumulative_gas_used: u64,
    coinbase_payment: U256,
}

fn execute<S, I>(
    db: &mut CacheDB<Arc<State<S>>>,
    post_state: &mut PostState,
    cfg_env: &CfgEnv,
    block_env: &BlockEnv,
    mut cumulative_gas_used: u64,
    txs: I,
) -> Result<Execution, EVMError<RethError>>
where
    S: StateProvider,
    I: IntoIterator<Item = TransactionSignedEcRecovered>,
{
    let block_num = block_env.number.to::<u64>();

    // determine the initial balance of the account at the coinbase address
    let coinbase_acct = db.basic(block_env.coinbase).map_err(EVMError::Database)?;
    let initial_coinbase_balance = coinbase_acct.map_or(U256::ZERO, |acct| acct.balance);

    for tx in txs {
        // construct EVM
        let tx_env = tx_env_with_recovered(&tx);
        let env = Env {
            cfg: cfg_env.clone(),
            block: block_env.clone(),
            tx: tx_env.clone(),
        };
        let mut evm = EVM::with_env(env);
        evm.database(&mut *db);

        // execute transaction
        let ResultAndState { result, state } = evm.transact()?;

        // commit changes to DB and post state
        commit_state_changes(db, post_state, block_num, state, true);

        cumulative_gas_used += result.gas_used();

        post_state.add_receipt(
            block_num,
            Receipt {
                tx_type: tx.tx_type(),
                success: result.is_success(),
                cumulative_gas_used,
                logs: result.logs().into_iter().map(into_reth_log).collect(),
            },
        );
    }

    // compute the coinbase payment
    let coinbase_payment =
        compute_coinbase_payment(&block_env.coinbase, initial_coinbase_balance, post_state);

    Ok(Execution {
        cumulative_gas_used,
        coinbase_payment,
    })
}

fn package_block<S: StateProvider>(
    state: S,
    attributes: &PayloadBuilderAttributes,
    extra_data: u128,
    block_env: &BlockEnv,
    txs: Vec<TransactionSigned>,
    post_state: PostState,
    cumulative_gas_used: u64,
) -> Result<SealedBlock, PayloadBuilderError> {
    let base_fee = block_env.basefee.to::<u64>();
    let block_num = block_env.number.to::<u64>();
    let block_gas_limit: u64 = block_env.gas_limit.try_into().unwrap_or(u64::MAX);

    // compute accumulators
    let receipts_root = post_state.receipts_root(block_num);
    let logs_bloom = post_state.logs_bloom(block_num);
    let transactions_root = proofs::calculate_transaction_root(&txs);
    let withdrawals_root = proofs::calculate_withdrawals_root(&attributes.withdrawals);
    let state_root = state.state_root(post_state)?;

    let header = Header {
        parent_hash: attributes.parent,
        ommers_hash: EMPTY_OMMER_ROOT,
        beneficiary: block_env.coinbase,
        state_root,
        transactions_root,
        receipts_root,
        withdrawals_root: Some(withdrawals_root),
        logs_bloom,
        difficulty: U256::ZERO,
        number: block_num,
        gas_limit: block_gas_limit,
        gas_used: cumulative_gas_used,
        timestamp: attributes.timestamp,
        mix_hash: attributes.prev_randao,
        nonce: BEACON_NONCE,
        base_fee_per_gas: Some(base_fee),
        blob_gas_used: None,
        excess_blob_gas: None,
        extra_data: extra_data.to_le_bytes().into(),
    };

    let block = Block {
        header,
        body: txs,
        ommers: vec![],
        withdrawals: Some(attributes.withdrawals.clone()),
    };
    Ok(block.seal_slow())
}

/// Computes the payment to `coinbase` based on `initial_balance` and `post_state`.
///
/// NOTE: If the ending balance is less than `initial_balance`, then we define the payment as zero.
/// If the account has been deleted, then we define the payment as zero. If the account was not
/// modified, then the payment will be zero.
fn compute_coinbase_payment(
    coinbase: &B160,
    initial_balance: U256,
    post_state: &PostState,
) -> U256 {
    match post_state.account(coinbase) {
        Some(Some(acct)) => acct.balance.saturating_sub(initial_balance),
        Some(None) => U256::ZERO,
        None => U256::ZERO,
    }
}

/// Constructs a transfer transaction to pay `amount` to `proposer`.
fn proposer_payment_tx(
    wallet: &LocalWallet,
    nonce: u64,
    base_fee: u64,
    chain_id: u64,
    proposer: &B160,
    amount: U256,
) -> TransactionSignedEcRecovered {
    let tx = Eip1559TransactionRequest::new()
        .from(wallet.address())
        .to(NameOrAddress::Address(ethers::types::H160::from_slice(
            proposer.as_bytes(),
        )))
        .gas(21000)
        .max_fee_per_gas(base_fee)
        .max_priority_fee_per_gas(0)
        .value(amount)
        .data(EthersBytes::default())
        .access_list(AccessList::default())
        .nonce(nonce)
        .chain_id(chain_id);
    let tx = TypedTransaction::Eip1559(tx);
    let signature = wallet.sign_transaction_sync(&tx).expect("can sign tx");
    let tx_encoded = tx.rlp_signed(&signature);
    let tx = TransactionSigned::decode_enveloped(Bytes::from(tx_encoded.as_ref()))
        .expect("can decode tx");
    tx.into_ecrecovered().expect("can recover tx signer")
}

#[cfg(test)]
mod tests {
    use super::*;

    use ethers::{
        signers::{LocalWallet, Signer},
        types::{
            transaction::eip2718::TypedTransaction, Eip1559TransactionRequest, NameOrAddress,
            H160 as EthersAddress,
        },
    };
    use reth_primitives::{Address, Bytes, TxType};
    use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
    use reth_revm::revm::primitives::{specification::SpecId, B256};

    const TRANSFER_GAS_LIMIT: u64 = 21000;

    fn env(coinbase: Address, basefee: U256) -> (CfgEnv, BlockEnv) {
        let cfg_env = CfgEnv {
            chain_id: U256::from(1),
            spec_id: SpecId::CANCUN,
            ..Default::default()
        };
        let block_env = BlockEnv {
            number: U256::ZERO,
            coinbase,
            timestamp: U256::ZERO,
            difficulty: U256::ZERO,
            prevrandao: Some(B256::random()),
            basefee,
            gas_limit: U256::from(15000000),
        };

        (cfg_env, block_env)
    }

    fn tx(
        from: &LocalWallet,
        to: EthersAddress,
        gas_limit: u64,
        max_fee_per_gas: u64,
        max_priority_fee_per_gas: u64,
        value: u64,
        nonce: u64,
    ) -> TransactionSignedEcRecovered {
        let tx = Eip1559TransactionRequest::new()
            .from(from.address())
            .to(NameOrAddress::Address(to))
            .gas(gas_limit)
            .max_fee_per_gas(max_fee_per_gas)
            .max_priority_fee_per_gas(max_priority_fee_per_gas)
            .value(value)
            .data(ethers::types::Bytes::default())
            .access_list(ethers::types::transaction::eip2930::AccessList::default())
            .nonce(nonce)
            .chain_id(from.chain_id());
        let tx = TypedTransaction::Eip1559(tx);
        let signature = from.sign_transaction_sync(&tx).expect("can sign tx");
        let tx_encoded = tx.rlp_signed(&signature);
        let tx = TransactionSigned::decode_enveloped(Bytes::from(tx_encoded.as_ref()))
            .expect("can decode tx");
        tx.into_ecrecovered().expect("can recover tx signer")
    }

    #[test]
    fn execute_transfer() {
        let state = MockEthProvider::default();

        // add sender account to state
        let sender_wallet = LocalWallet::new(&mut rand::thread_rng());
        let initial_sender_balance = 10000000;
        let sender_nonce = 0;
        let sender_account = ExtendedAccount::new(sender_nonce, U256::from(initial_sender_balance));
        state.add_account(sender_wallet.address().into(), sender_account);

        let state = State::new(state);
        let mut db = CacheDB::new(Arc::new(state));
        let mut post_state = PostState::default();

        // builder will be the coinbase (i.e. beneficiary)
        let builder_wallet = LocalWallet::new(&mut rand::thread_rng());

        let (cfg_env, block_env) = env(builder_wallet.address().into(), U256::ZERO);

        let receiver_wallet = LocalWallet::new(&mut rand::thread_rng());
        let transfer_amount = 100;
        let max_priority_fee = 100;
        let max_fee = block_env.basefee.to::<u64>() + max_priority_fee;

        // construct the transfer transaction for execution
        let transfer_tx = tx(
            &sender_wallet,
            receiver_wallet.address(),
            TRANSFER_GAS_LIMIT,
            max_fee,
            max_priority_fee,
            transfer_amount,
            sender_nonce,
        );

        // execute the transfer transaction
        let execution = execute(
            &mut db,
            &mut post_state,
            &cfg_env,
            &block_env,
            0,
            Some(transfer_tx),
        )
        .expect("execution doesn't fail");
        let Execution {
            cumulative_gas_used,
            coinbase_payment,
        } = execution;

        // expected gas usage is the transfer transaction's gas limit
        let expected_cumulative_gas_used = TRANSFER_GAS_LIMIT;

        // check post state contains transaction receipt
        let receipt = post_state
            .receipts(block_env.number.to::<u64>())
            .first()
            .expect("post state contains receipt");
        assert!(receipt.success);
        assert_eq!(receipt.tx_type, TxType::EIP1559);
        assert_eq!(receipt.cumulative_gas_used, expected_cumulative_gas_used);

        // check post-execution sender balance
        let sender_account = post_state
            .account(&Address::from(sender_wallet.address()))
            .expect("sender account touched")
            .expect("sender account not destroyed");
        let expected_sender_balance =
            initial_sender_balance - transfer_amount - (max_fee * receipt.cumulative_gas_used);
        assert_eq!(sender_account.balance, U256::from(expected_sender_balance));

        // check post-execution receiver balance
        let receiver_account = post_state
            .account(&Address::from(receiver_wallet.address()))
            .expect("receiver account touched")
            .expect("receiver account not destroyed");
        assert_eq!(receiver_account.balance, U256::from(transfer_amount));

        // check gas usage
        assert_eq!(cumulative_gas_used, expected_cumulative_gas_used);

        // check coinbase payment
        let expected_coinbase_payment = cumulative_gas_used * max_priority_fee;
        let builder_account = post_state
            .account(&Address::from(builder_wallet.address()))
            .expect("builder account touched")
            .expect("builder account not destroyed");
        assert_eq!(
            builder_account.balance,
            U256::from(expected_coinbase_payment)
        );
        assert_eq!(coinbase_payment, U256::from(expected_coinbase_payment));
    }

    #[test]
    fn execute_coinbase_transfer() {
        let state = MockEthProvider::default();

        // populate coinbase transfer smart contract in the DB
        //
        // h/t lightclient: https://github.com/lightclient/sendall
        let contract_addr = Address::random();
        let bytecode = vec![0x5f, 0x5f, 0x5f, 0x5f, 0x47, 0x41, 0x5a, 0xf1, 0x00];
        let contract_acct = ExtendedAccount::new(0, U256::ZERO).with_bytecode(bytecode.into());
        state.add_account(contract_addr, contract_acct);

        // add caller account to state
        let sender_wallet = LocalWallet::new(&mut rand::thread_rng());
        let initial_sender_balance = 10000000;
        let sender_nonce = 0;
        let sender_account = ExtendedAccount::new(sender_nonce, U256::from(initial_sender_balance));
        state.add_account(sender_wallet.address().into(), sender_account);

        let state = State::new(state);
        let mut db = CacheDB::new(Arc::new(state));
        let mut post_state = PostState::default();

        // builder will be the coinbase (i.e. beneficiary)
        let builder_wallet = LocalWallet::new(&mut rand::thread_rng());

        let (cfg_env, block_env) = env(builder_wallet.address().into(), U256::ZERO);

        let tx_value = 100;
        let tx_gas_limit = 84000;
        let max_priority_fee = 100;
        let max_fee = block_env.basefee.to::<u64>() + max_priority_fee;

        // construct the contract call transaction for execution
        let call_tx = tx(
            &sender_wallet,
            EthersAddress(*contract_addr),
            tx_gas_limit,
            max_fee,
            max_priority_fee,
            tx_value,
            sender_nonce,
        );

        let execution = execute(
            &mut db,
            &mut post_state,
            &cfg_env,
            &block_env,
            0,
            Some(call_tx),
        )
        .expect("execution doesn't fail");
        let Execution {
            coinbase_payment,
            cumulative_gas_used,
        } = execution;

        // check coinbase payment
        let expected_coinbase_payment = tx_value + (cumulative_gas_used * max_priority_fee);
        let builder_account = post_state
            .account(&Address::from(builder_wallet.address()))
            .expect("builder account touched")
            .expect("builder account not destroyed");
        assert_eq!(
            builder_account.balance,
            U256::from(expected_coinbase_payment)
        );
        assert_eq!(coinbase_payment, U256::from(expected_coinbase_payment));
    }
}
