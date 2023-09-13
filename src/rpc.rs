use jsonrpsee::{
    core::{async_trait, RpcResult},
    proc_macros::rpc,
};
use mev_rs::types::{capella::ExecutionPayload, SignedBidSubmission};
use reth_primitives::{Block, Bytes, FromRecoveredTransaction, TransactionSigned, H256};
use reth_provider::{
    BlockReaderIdExt, ChainSpecProvider, ChangeSetReader, EvmEnvProvider, StateProviderFactory,
};
use reth_revm::{database::State, executor::Executor};
use reth_revm_primitives::db::CacheDB;
use reth_rpc::eth::error::{EthApiError, RpcPoolError};
use reth_transaction_pool::{TransactionOrigin, TransactionPool};

#[rpc(server, namespace = "eth")]
pub trait EthExtApi {
    #[method(name = "sendPrivateTransaction")]
    async fn send_private_transaction(&self, tx: Bytes) -> RpcResult<H256>;
}

pub struct EthExt<Pool> {
    pool: Pool,
}

impl<Pool> EthExt<Pool> {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl<Pool> EthExtApiServer for EthExt<Pool>
where
    Pool: TransactionPool + Clone + 'static,
{
    async fn send_private_transaction(&self, tx: Bytes) -> RpcResult<H256> {
        if tx.is_empty() {
            return Err(EthApiError::EmptyRawTransactionData)?;
        }
        let tx = TransactionSigned::decode_enveloped(tx)
            .or(Err(EthApiError::FailedToDecodeSignedTransaction))?;
        let tx = tx
            .into_ecrecovered()
            .ok_or(EthApiError::InvalidTransactionSignature)?;
        let tx = <Pool::Transaction>::from_recovered_transaction(tx);
        let hash = self
            .pool
            .add_transaction(TransactionOrigin::Private, tx)
            .await
            .map_err(RpcPoolError::from)?;
        Ok(hash)
    }
}

#[rpc(server, namespace = "relay")]
pub trait RelayApi {
    #[method(name = "validateBid")]
    async fn validate_bid(&self, bid: SignedBidSubmission) -> RpcResult<bool>;
}

pub struct Relay<Provider> {
    provider: Provider,
}

impl<Provider> Relay<Provider> {
    pub fn new(provider: Provider) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<Provider> RelayApiServer for Relay<Provider>
where
    Provider: BlockReaderIdExt
        + StateProviderFactory
        + EvmEnvProvider
        + ChainSpecProvider
        + ChangeSetReader
        + Clone
        + Unpin
        + 'static,
{
    async fn validate_bid(&self, bid: SignedBidSubmission) -> RpcResult<bool> {
        let block = execution_payload_to_block(bid.execution_payload);

        // verify signature

        // verify registration

        // verify execution
        let state = self
            .provider
            .state_by_block_hash(H256::from_slice(bid.message.parent_hash.as_slice()))
            .unwrap();
        let state = State::new(state);
        let db = CacheDB::new(state);
        let mut executor = Executor::new(self.provider.chain_spec(), db);
        let total_difficulty = self
            .provider
            .header_td_by_number(block.number)
            .unwrap()
            .unwrap();
        let (post_state, cumulative_gas_used) = executor
            .execute_transactions(&block, total_difficulty, None)
            .unwrap();

        // verify gas limit

        // verify payment

        Ok(true)
    }
}

fn execution_payload_to_block(payload: ExecutionPayload) -> Block {
    Block::default()
}
