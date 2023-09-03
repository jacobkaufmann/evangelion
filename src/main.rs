mod builder;
mod bundle;

use std::collections::HashSet;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beacon_api_client::mainnet::Client as BeaconClient;
use clap::Parser;
use ethereum_consensus::{
    capella::mainnet::ExecutionPayload,
    clock,
    configs::mainnet::SECONDS_PER_SLOT,
    crypto::SecretKey,
    phase0::mainnet::SLOTS_PER_EPOCH,
    primitives::{BlsPublicKey, Bytes32, ExecutionAddress, Slot},
    ssz::{ByteList, ByteVector},
    state_transition::Context,
};
use ethers::signers::{LocalWallet, Signer};
use mev_rs::{
    blinded_block_relayer::{BlindedBlockRelayer, Client as RelayClient},
    signing::sign_builder_message,
    types::{BidTrace, SignedBidSubmission},
    ProposerScheduler, ValidatorRegistry,
};
use reth::cli::{
    config::PayloadBuilderConfig,
    ext::{RethCliExt, RethNodeCommandConfig},
    Cli,
};
use reth_payload_builder::{
    BuiltPayload, PayloadBuilderAttributes, PayloadBuilderHandle, PayloadBuilderService,
};
use reth_primitives::{Bloom, Bytes, Chain, ChainSpec, SealedBlock, H160, H256, U256};
use reth_rpc_types::engine::PayloadAttributes;
use ssz_rs::prelude::*;
use tokio::{
    sync::mpsc,
    time::{interval, interval_at},
};
use url::Url;

use builder::{Builder, BuilderConfig};

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
        Provider: reth_provider::StateProviderFactory
            + reth_provider::BlockReaderIdExt
            + Clone
            + Unpin
            + 'static,
        Pool: reth::transaction_pool::TransactionPool + Unpin + 'static,
        Tasks: reth::tasks::TaskSpawner + Clone + Unpin + 'static,
    {
        // only allow sepolia
        assert_eq!(chain_spec.chain(), Chain::sepolia());

        // beacon client
        tracing::info!("EVA's beacon API endpoint {}", self.beacon_endpoint);
        let beacon_client = Arc::new(BeaconClient::new(self.beacon_endpoint.clone()));

        // relay client
        tracing::info!("EVA's relay API endpoint {}", self.relay_endpoint);
        let relay_client = BeaconClient::new(self.relay_endpoint.clone());
        let relay_client = Arc::new(RelayClient::new(relay_client));

        // builder wallet
        let extra_data = Bytes::from(conf.extradata().as_bytes());
        let wallet = LocalWallet::from_str(&self.wallet_sk).expect("wallet secret key valid");

        // builder BLS key
        let bls_sk = SecretKey::try_from(self.boost_bls_sk.clone()).expect("BLS secret key valid");
        let bls_sk = Arc::new(bls_sk);
        let bls_pk = Arc::new(bls_sk.public_key());

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
        let build_deadline = Duration::from_secs(12);
        let config = BuilderConfig {
            extra_data,
            wallet,
            deadline: build_deadline,
        };
        let builder = Builder::new(config, chain_spec.deref().clone(), provider, jobs_tx, pool);
        builder.start(bundles, events);

        // spawn payload builder service to drive payload build jobs forward
        tracing::info!("spawning payload builder service");
        let (payload_service, payload_builder) = PayloadBuilderService::new(builder);
        executor.spawn_critical("payload builder service", Box::pin(payload_service));

        // spawn task to participate in mev-boost auction
        //
        // we need to clone the payload builder handle to gain access to the underlying builder
        tracing::info!("spawning mev-boost auction");
        let other_payload_builder = payload_builder.clone();
        let other_executor = executor.clone();
        executor.spawn_critical("mev-boost auction", Box::pin(async move {
            // construct types to manage proposer preferences
            let validators = Arc::new(ValidatorRegistry::new(beacon_client.deref().clone()));
            let scheduler = Arc::new(ProposerScheduler::new(beacon_client.deref().clone()));


            let context = Arc::new(Context::for_sepolia());

            // time related values
            let clock = clock::for_sepolia();

            // refresh the consensus and mev-boost info each epoch
            let seconds_per_epoch = SECONDS_PER_SLOT * SLOTS_PER_EPOCH;
            let validator_info_refresh_interval = Duration::from_secs(seconds_per_epoch);
            let mut validator_info_refresh_interval = interval(validator_info_refresh_interval);

            // TODO: clean up periodically, otherwise grows indefinitely
            let mut initiated_jobs = HashSet::new();

            loop {
                tokio::select! {
                    _ = validator_info_refresh_interval.tick() => {
                        tracing::info!("refreshing consensus and mev-boost info");

                        // load validators
                        tracing::info!("loading validators...");
                        if let Err(err) = validators.load().await {
                            tracing::error!("unable to load validators {err}, quitting builder...");
                        }
                        tracing::info!("successfully loaded validators");

                        // retrieve proposer duties for the current epoch
                        tracing::info!("retrieving proposer duties...");
                        let epoch = clock.current_epoch().expect("beyond genesis");
                        if let Err(err) = scheduler.fetch_duties(epoch).await {
                            tracing::error!("unable to retrieve proposer duties {err}");
                        }
                        tracing::info!("successfully retrieved proposer duties");

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
                        let payload_slot = clock.slot_at_time(attrs.timestamp).expect("beyond genesis");

                        // if this is a job we initiated below, then move on
                        if initiated_jobs.contains(&payload_id) {
                            continue;
                        }

                        // cancel the job, since we only want to keep jobs that we initiated
                        tracing::info!(
                            payload = %payload_id,
                            slot = %payload_slot,
                            "cancelling non-mev-boost payload job"
                        );
                        cancel.cancel();

                        // look up the proposer preferences for the slot if available
                        tracing::info!(
                            payload = %payload_id,
                            slot = %payload_slot,
                            "looking up mev-boost registration for proposer"
                        );
                        let proposer = match scheduler.get_proposer_for(payload_slot) {
                            Ok(proposer) => proposer,
                            Err(err) => {
                                tracing::warn!(
                                    payload = %payload_id,
                                    slot = %payload_slot,
                                    "unable to retrieve proposer for slot {err}, not bidding for slot"
                                );
                                continue;
                            }
                        };
                        let prefs = match validators.get_preferences(&proposer) {
                            Some(prefs) => {
                                tracing::info!(
                                    payload = %payload_id,
                                    slot = %payload_slot,
                                    "found mev-boost registration for proposer {prefs:?}"
                                );
                                prefs
                            }
                            None => {
                                tracing::info!(
                                    payload = %payload_id,
                                    slot = %payload_slot,
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

                            match other_payload_builder.new_payload(attrs.clone()).await {
                                Ok(id) => payload_id = id,
                                Err(err) => {
                                    tracing::error!(
                                        slot = %payload_slot,
                                        "unable to initiate new payload job {err}, not bidding for slot"
                                    );
                                    continue;
                                }
                            }
                            initiated_jobs.insert(payload_id);
                            tracing::info!(
                                payload = %payload_id,
                                slot = %payload_slot,
                                "successfully initiated new payload job for mev-boost auction"
                            );
                        }

                        // spawn a task to periodically poll the payload job and submit bids to the
                        // mev-boost relay
                        let proposer_pk = Arc::new(proposer);
                        let proposer_fee_recipient = attrs.suggested_fee_recipient;
                        let inner_payload_builder = other_payload_builder.clone();
                        let inner_relay_client = Arc::clone(&relay_client);
                        let inner_bls_pk = Arc::clone(&bls_pk);
                        let inner_bls_sk = Arc::clone(&bls_sk);
                        let inner_context = Arc::clone(&context);
                        other_executor.spawn(Box::pin(async move {
                            // starting 500ms from now, poll the job every 500ms, and only poll for the duration of a slot
                            let payload_poll_interval = Duration::from_millis(500);
                            let start = Instant::now() + payload_poll_interval;
                            let mut payload_poll_interval = interval_at(start.into(), payload_poll_interval);

                            // keep track of the highest bid we have sent to the mev-boost relay
                            let mut highest_bid = U256::ZERO;

                            // TODO: watch auction so that we can terminate early and so that we
                            // can know whether we won or lost
                            loop {
                                payload_poll_interval.tick().await;

                                // poll the job for the best available payload
                                match inner_payload_builder.best_payload(payload_id).await {
                                    Some(Ok(payload)) => {
                                        if payload.fees() > highest_bid {
                                            tracing::info!(
                                                payload = %payload.id(),
                                                "submitting bid for payload with higher value {} to relay",
                                                payload.fees()
                                            );

                                            // construct signed bid
                                            let mut message = built_payload_to_bid_trace(
                                                &payload,
                                                payload_slot,
                                                inner_bls_pk.clone(),
                                                proposer_pk.clone(),
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

                                            if let Err(err) = inner_relay_client.submit_bid(&submission).await {
                                                tracing::warn!(
                                                    payload = %payload_id,
                                                    "unable to submit higher bid to relay {err}"
                                                );
                                            } else {
                                                highest_bid = payload.fees();
                                                tracing::info!(
                                                    payload = %payload_id,
                                                    "successfully submitted bid to relay with value {highest_bid}"
                                                );
                                            }
                                        }
                                    }
                                    Some(Err(err)) => {
                                        tracing::warn!(
                                            payload = %payload_id,
                                            "unable to retrieve best payload from build job {err}"
                                        );
                                    }
                                    None => {}
                                }
                            }
                        }));
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
