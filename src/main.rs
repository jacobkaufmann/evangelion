mod builder;
mod bundle;
mod rpc;

use std::collections::HashSet;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beacon_api_client::mainnet::Client as BeaconClient;
use clap::Parser;
use ethereum_consensus::{
    capella::mainnet::ExecutionPayload,
    clock::{self, Clock, SystemTimeProvider},
    configs::mainnet::SECONDS_PER_SLOT,
    crypto::SecretKey,
    phase0::mainnet::SLOTS_PER_EPOCH,
    primitives::{BlsPublicKey, Bytes32, ExecutionAddress, Slot},
    ssz::{ByteList, ByteVector},
    state_transition::Context,
};
use ethers::signers::{LocalWallet, Signer};
use futures_util::StreamExt;
use mev_rs::{
    blinded_block_relayer::{BlindedBlockRelayer, Client as RelayClient},
    signing::sign_builder_message,
    types::{BidTrace, SignedBidSubmission},
    ProposerScheduler, ValidatorRegistry,
};
use reth::{
    cli::config::RethRpcConfig,
    cli::{
        config::PayloadBuilderConfig,
        ext::{RethCliExt, RethNodeCommandConfig},
        Cli,
    },
    network::{NetworkInfo, Peers},
    rpc::builder::{RethModuleRegistry, TransportRpcModules},
    tasks::TaskSpawner,
};
use reth_payload_builder::{
    BuiltPayload, PayloadBuilderAttributes, PayloadBuilderHandle, PayloadBuilderService,
};
use reth_primitives::{Bloom, Bytes, Chain, ChainSpec, SealedBlock, H160, H256, U256};
use reth_provider::{
    BlockReaderIdExt, CanonStateSubscriptions, ChainSpecProvider, ChangeSetReader, EvmEnvProvider,
    StateProviderFactory,
};
use reth_rpc_types::engine::PayloadAttributes;
use reth_transaction_pool::TransactionPool;
use ssz_rs::prelude::*;
use tokio::{
    sync::mpsc,
    time::{interval, interval_at},
};
use tokio_util::time::DelayQueue;
use url::Url;

use builder::{Builder, BuilderConfig};
use rpc::{EthExt, EthExtApiServer};

#[derive(Debug, Clone, clap::Args)]
struct EvaRethNodeCommandExt {
    /// hex-encoded secret key corresponding to builder's wallet
    #[clap(long, required(true))]
    pub wallet_sk: String,
    /// hex-encoded secret key corresponding to builder's mev-boost BLS public key
    #[clap(long, required(true))]
    pub boost_bls_sk: String,
    /// URL endpoint of beacon API
    #[clap(long, required(true))]
    pub beacon_endpoint: Url,
    /// URL endpoint of mev-boost relay
    #[clap(long, required(true))]
    pub relay_endpoint: Url,
}

impl RethNodeCommandConfig for EvaRethNodeCommandExt {
    fn extend_rpc_modules<Conf, Provider, Pool, Network, Tasks, Events>(
        &mut self,
        _config: &Conf,
        registry: &mut RethModuleRegistry<Provider, Pool, Network, Tasks, Events>,
        modules: &mut TransportRpcModules<()>,
    ) -> eyre::Result<()>
    where
        Conf: RethRpcConfig,
        Provider: BlockReaderIdExt
            + StateProviderFactory
            + EvmEnvProvider
            + ChainSpecProvider
            + ChangeSetReader
            + Clone
            + Unpin
            + 'static,
        Pool: TransactionPool + Clone + 'static,
        Network: NetworkInfo + Peers + Clone + 'static,
        Tasks: TaskSpawner + Clone + 'static,
        Events: CanonStateSubscriptions + Clone + 'static,
    {
        let ext = EthExt::new(registry.pool().clone());
        modules.merge_configured(ext.into_rpc())?;

        Ok(())
    }

    fn spawn_payload_builder_service<Conf, Provider, Pool, Tasks>(
        &mut self,
        conf: &Conf,
        provider: Provider,
        pool: Pool,
        executor: Tasks,
        chain_spec: Arc<ChainSpec>,
    ) -> eyre::Result<PayloadBuilderHandle>
    where
        Conf: PayloadBuilderConfig,
        Provider: StateProviderFactory + BlockReaderIdExt + Clone + Unpin + 'static,
        Pool: TransactionPool + Unpin + 'static,
        Tasks: TaskSpawner + Clone + Unpin + 'static,
    {
        // beacon client
        tracing::info!("EVA's beacon API endpoint {}", self.beacon_endpoint);
        let beacon_client = Arc::new(BeaconClient::new(self.beacon_endpoint.clone()));

        // relay client
        tracing::info!("EVA's relay API endpoint {}", self.relay_endpoint);
        let relay_client = BeaconClient::new(self.relay_endpoint.clone());
        let relay_client = Arc::new(RelayClient::new(relay_client));

        // builder extra data
        let extra_data = Bytes::from(conf.extradata().as_bytes());

        // builder wallet
        let wallet = LocalWallet::from_str(&self.wallet_sk).expect("wallet secret key valid");

        // builder BLS key
        let bls_sk = SecretKey::try_from(self.boost_bls_sk.clone()).expect("BLS secret key valid");
        let bls_sk = Arc::new(bls_sk);
        let bls_pk = Arc::new(bls_sk.public_key());

        // chain info
        let (context, clock) = context_and_clock(&chain_spec).expect("recognized chain spec");
        let context = Arc::new(context);

        tracing::info!(
            "EVA booting up...\n\textra data {}\n\texecution address {}\n\tBLS public key {}",
            extra_data,
            wallet.address(),
            bls_pk,
        );

        // TODO: spawn server that will listen for bundles and channel them through the sender
        let (_, bundles) = mpsc::unbounded_channel();

        // TODO: maybe we don't need events here. cancellations?
        let (_, events) = mpsc::unbounded_channel();

        let (jobs_tx, mut jobs_rx) = mpsc::unbounded_channel();

        // construct and start the builder
        tracing::info!("spawning builder");
        let build_deadline = Duration::from_secs(SECONDS_PER_SLOT);
        let build_interval = Duration::from_millis(500);
        let config = BuilderConfig {
            extra_data,
            wallet,
            deadline: build_deadline,
            interval: build_interval,
        };
        let builder = Builder::new(config, chain_spec.deref().clone(), provider, pool, jobs_tx);
        builder.start(bundles, events);

        // spawn payload builder service to drive payload build jobs forward
        tracing::info!("spawning payload builder service");
        let (payload_service, payload_builder) = PayloadBuilderService::new(builder);
        executor.spawn_critical("payload builder service", Box::pin(payload_service));

        // spawn task to participate in mev-boost auction
        tracing::info!("spawning mev-boost auction");
        let other_payload_builder = payload_builder.clone();
        let other_executor = executor.clone();
        executor.spawn_critical("mev-boost auction", Box::pin(async move {
            // construct types to manage proposer preferences
            let validators = Arc::new(ValidatorRegistry::new(beacon_client.deref().clone()));
            let scheduler = Arc::new(ProposerScheduler::new(beacon_client.deref().clone()));

            // refresh the consensus and mev-boost info each epoch
            let seconds_per_epoch = SECONDS_PER_SLOT * SLOTS_PER_EPOCH;
            let validator_info_refresh_interval = Duration::from_secs(seconds_per_epoch);
            let mut validator_info_refresh_interval = interval(validator_info_refresh_interval);

            // keep track of the jobs that we initiate
            let mut initiated_jobs = HashSet::new();
            let mut jobs_removal_queue = DelayQueue::new();

            // keep track of the current slot
            let mut current_slot = clock.current_slot().expect("beyond genesis");
            let slots = clock.stream_slots();
            tokio::pin!(slots);

            loop {
                tokio::select! {
                    Some(slot) = slots.next() => {
                        current_slot = slot;
                    }
                    _ = validator_info_refresh_interval.tick() => {
                        tracing::info!("refreshing consensus and mev-boost info...");

                        // load validators
                        tracing::info!("loading validators...");
                        if let Err(err) = validators.load().await {
                            tracing::error!("unable to load validators {err}");
                        } else {
                            tracing::info!("successfully loaded validators");
                        }

                        // retrieve proposer duties for the current epoch
                        tracing::info!("retrieving proposer duties...");
                        let epoch = clock.current_epoch().expect("beyond genesis");
                        if let Err(err) = scheduler.fetch_duties(epoch).await {
                            tracing::error!("unable to retrieve proposer duties {err}");
                        } else {
                            tracing::info!("successfully retrieved proposer duties");
                        }

                        // retrieve proposer schedule from relay and validate registrations
                        tracing::info!("retrieving proposer schedule...");
                        match relay_client.get_proposal_schedule().await {
                            Ok(schedule) => {
                                tracing::info!("successfully retrieved proposer schedule, validating registrations...");
                                let timestamp = clock::get_current_unix_time_in_secs();
                                let mut registrations: Vec<_> = schedule.into_iter().map(|entry| entry.entry).collect();
                                if let Err(err) = validators.validate_registrations(&mut registrations, timestamp, &context) {
                                    tracing::error!("invalid registration {err}");
                                } else {
                                    tracing::info!("successfully validated registrations");
                                }
                            }
                            Err(err) => {
                                tracing::error!("unable to load proposer schedule from relay {err}");
                            }
                        }
                    }
                    Some((mut attrs, cancel)) = jobs_rx.recv() => {
                        let mut payload_id = attrs.id;

                        // if this is a job we initiated below, then move on
                        if initiated_jobs.contains(&payload_id) {
                            continue;
                        }

                        let payload_slot = clock.slot_at_time(attrs.timestamp).expect("beyond genesis");

                        // cancel the job, since we only want to keep jobs that we initiated
                        tracing::debug!(
                            slot = %payload_slot,
                            payload = %payload_id,
                            "cancelling non-mev-boost payload job"
                        );
                        cancel.cancel();

                        // if the payload attributes are for a slot prior to the current slot, then
                        // do not attempt to create a new job
                        if payload_slot < current_slot {
                            continue;
                        }

                        // look up the proposer preferences for the slot if available
                        let proposer = match scheduler.get_proposer_for(payload_slot) {
                            Ok(proposer) => proposer,
                            Err(err) => {
                                tracing::warn!(
                                    slot = %payload_slot,
                                    "unable to retrieve proposer for slot {err}, not bidding for slot"
                                );
                                continue;
                            }
                        };
                        let prefs = match validators.get_preferences(&proposer) {
                            Some(prefs) => {
                                tracing::info!(
                                    slot = %payload_slot,
                                    proposer = %proposer,
                                    "found mev-boost registration for proposer {prefs:?}"
                                );
                                prefs
                            }
                            None => {
                                tracing::info!(
                                    slot = %payload_slot,
                                    proposer = %proposer,
                                    "mev-boost registration not found for proposer, not bidding for slot"
                                );
                                continue;
                            }
                        };

                        // TODO: pass gas limit to builder somehow

                        // if the fee recipient in the payload attributes does not match the
                        // registration, then initiate a new payload build job with the proper fee
                        // recipient
                        let preferred_fee_recipient = H160::from_slice(prefs.fee_recipient.as_slice());
                        if attrs.suggested_fee_recipient != preferred_fee_recipient {
                            let attributes = PayloadAttributes {
                                timestamp: attrs.timestamp.into(),
                                prev_randao: attrs.prev_randao,
                                suggested_fee_recipient: preferred_fee_recipient,
                                withdrawals: Some(attrs.withdrawals.clone()),
                                parent_beacon_block_root: None,
                            };
                            attrs = PayloadBuilderAttributes::new(attrs.parent, attributes);
                            payload_id = attrs.payload_id();

                            // if we already initiated a job with identical attributes, then move on
                            if initiated_jobs.contains(&payload_id) {
                                continue;
                            }

                            if let Err(err) = other_payload_builder.new_payload(attrs.clone()).await {
                                tracing::error!(
                                    slot = %payload_slot,
                                    "unable to initiate new payload job {err}, not bidding for slot"
                                );
                                continue;
                            }

                            initiated_jobs.insert(payload_id);
                            jobs_removal_queue.insert(payload_id, Duration::from_secs(60));
                            tracing::info!(
                                slot = %payload_slot,
                                proposer = %proposer,
                                payload = %payload_id,
                                "successfully initiated new payload job for mev-boost auction"
                            );
                        }

                        // spawn a task to periodically poll the payload job and submit bids to the
                        // mev-boost relay
                        let proposer = Arc::new(proposer);
                        let proposer_fee_recipient = attrs.suggested_fee_recipient;
                        let inner_payload_builder = other_payload_builder.clone();
                        let inner_relay_client = Arc::clone(&relay_client);
                        let inner_bls_pk = Arc::clone(&bls_pk);
                        let inner_bls_sk = Arc::clone(&bls_sk);
                        let inner_context = Arc::clone(&context);
                        other_executor.spawn(Box::pin(async move {
                            // starting 500ms from now, poll the job every 500ms
                            let payload_poll_interval = Duration::from_millis(500);
                            let start = Instant::now() + payload_poll_interval;
                            let mut payload_poll_interval = interval_at(start.into(), payload_poll_interval);

                            // keep track of the highest bid we have sent to the mev-boost relay
                            let mut highest_bid = U256::ZERO;

                            // TODO: watch auction so that we can terminate early and so that we
                            // can know whether we won or lost
                            let start = Instant::now();
                            loop {
                                // only poll the job for the duration of a slot
                                if start.elapsed() > Duration::from_secs(SECONDS_PER_SLOT) {
                                    break;
                                }

                                payload_poll_interval.tick().await;

                                // poll the job for the best available payload
                                match inner_payload_builder.best_payload(payload_id).await {
                                    Some(Ok(payload)) => {
                                        if payload.fees() > highest_bid {
                                            tracing::info!(
                                                slot = %payload_slot,
                                                proposer = %proposer,
                                                payload = %payload_id,
                                                "submitting bid for payload with higher value {} to relay",
                                                payload.fees()
                                            );

                                            // construct signed bid
                                            let mut message = built_payload_to_bid_trace(
                                                &payload,
                                                payload_slot,
                                                inner_bls_pk.clone(),
                                                proposer.clone(),
                                                to_bytes20(proposer_fee_recipient)
                                            );
                                            let execution_payload = block_to_execution_payload(payload.block());
                                            let signature = sign_builder_message(
                                                &mut message,
                                                &inner_bls_sk,
                                                &inner_context
                                            ).expect("can sign with BLS sk");
                                            let submission = SignedBidSubmission {
                                                message,
                                                execution_payload,
                                                signature,
                                            };

                                            // submit signed bid
                                            if let Err(err) = inner_relay_client.submit_bid(&submission).await {
                                                tracing::warn!(
                                                    slot = %payload_slot,
                                                    proposer = %proposer,
                                                    payload = %payload_id,
                                                    "unable to submit higher bid to relay {err}"
                                                );
                                            } else {
                                                highest_bid = payload.fees();
                                                tracing::info!(
                                                    slot = %payload_slot,
                                                    proposer = %proposer,
                                                    payload = %payload_id,
                                                    "successfully submitted bid to relay with value {highest_bid}"
                                                );
                                            }
                                        }
                                    }
                                    Some(Err(err)) => {
                                        tracing::warn!(
                                            slot = %payload_slot,
                                            proposer = %proposer,
                                            payload = %payload_id,
                                            "unable to retrieve best payload from build job {err}"
                                        );
                                    }
                                    None => {
                                        tracing::warn!(
                                            slot = %payload_slot,
                                            proposer = %proposer,
                                            payload = %payload_id,
                                            "payload not available for submission"
                                        );
                                    }
                                }
                            }
                        }));
                    }
                    Some(job) = jobs_removal_queue.next() => {
                        initiated_jobs.remove(job.get_ref());
                    }
                }
            }
        }));

        Ok(payload_builder)
    }
}

struct EvaRethCliExt;

impl RethCliExt for EvaRethCliExt {
    type Node = EvaRethNodeCommandExt;
}

fn main() {
    Cli::<EvaRethCliExt>::parse().run().unwrap();
}

// COMPATIBILITY
//
// TODO: move into separate module

fn context_and_clock<T: AsRef<ChainSpec>>(
    chain: T,
) -> Option<(Context, Clock<SystemTimeProvider>)> {
    let chain = chain.as_ref().chain();
    if chain == Chain::mainnet() {
        Some((Context::for_mainnet(), clock::for_mainnet()))
    } else if chain == Chain::goerli() {
        Some((Context::for_goerli(), clock::for_goerli()))
    } else if chain == Chain::sepolia() {
        Some((Context::for_sepolia(), clock::for_sepolia()))
    } else {
        None
    }
}

fn to_bytes32(value: H256) -> Bytes32 {
    Bytes32::try_from(value.as_bytes()).unwrap()
}

fn to_bytes20(value: H160) -> ExecutionAddress {
    ExecutionAddress::try_from(value.as_bytes()).unwrap()
}

fn to_byte_vector(value: Bloom) -> ByteVector<256> {
    ByteVector::<256>::try_from(value.as_bytes()).unwrap()
}

fn to_u256(value: U256) -> ssz_rs::U256 {
    ssz_rs::U256::try_from_bytes_le(&value.to_le_bytes::<32>()).unwrap()
}

fn built_payload_to_bid_trace<B: AsRef<BlsPublicKey>, P: AsRef<BlsPublicKey>>(
    payload: &BuiltPayload,
    slot: Slot,
    builder_public_key: B,
    proposer_public_key: P,
    proposer_fee_recipient: ExecutionAddress,
) -> BidTrace {
    BidTrace {
        slot,
        parent_hash: to_bytes32(payload.block().parent_hash),
        block_hash: to_bytes32(payload.block().hash),
        builder_public_key: builder_public_key.as_ref().clone(),
        proposer_public_key: proposer_public_key.as_ref().clone(),
        proposer_fee_recipient,
        gas_limit: payload.block().gas_limit,
        gas_used: payload.block().gas_used,
        value: to_u256(payload.fees()),
    }
}

fn block_to_execution_payload(block: &SealedBlock) -> ExecutionPayload {
    let header = &block.header;
    let transactions = block
        .body
        .iter()
        .map(|t| {
            ethereum_consensus::capella::mainnet::Transaction::try_from(
                t.envelope_encoded().as_ref(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let withdrawals = block
        .withdrawals
        .as_ref()
        .unwrap()
        .iter()
        .map(|w| ethereum_consensus::capella::mainnet::Withdrawal {
            index: w.index as usize,
            validator_index: w.validator_index as usize,
            address: to_bytes20(w.address),
            amount: w.amount,
        })
        .collect::<Vec<_>>();
    ExecutionPayload {
        parent_hash: to_bytes32(header.parent_hash),
        fee_recipient: to_bytes20(header.beneficiary),
        state_root: to_bytes32(header.state_root),
        receipts_root: to_bytes32(header.receipts_root),
        logs_bloom: to_byte_vector(header.logs_bloom),
        prev_randao: to_bytes32(header.mix_hash),
        block_number: header.number,
        gas_limit: header.gas_limit,
        gas_used: header.gas_used,
        timestamp: header.timestamp,
        extra_data: ByteList::try_from(header.extra_data.as_ref()).unwrap(),
        base_fee_per_gas: ssz_rs::U256::from(header.base_fee_per_gas.unwrap_or_default()),
        block_hash: to_bytes32(block.hash()),
        transactions: TryFrom::try_from(transactions).unwrap(),
        withdrawals: TryFrom::try_from(withdrawals).unwrap(),
    }
}
