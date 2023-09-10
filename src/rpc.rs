use jsonrpsee::{
    core::{async_trait, RpcResult},
    proc_macros::rpc,
};
use reth_primitives::{Bytes, FromRecoveredTransaction, TransactionSigned, H256};
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
