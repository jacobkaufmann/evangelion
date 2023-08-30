use std::collections::HashSet;
use std::ops::RangeInclusive;

use reth_primitives::{BlockNumber, TransactionSignedEcRecovered};

pub mod pool;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct BundleCompact(pub Vec<TransactionSignedEcRecovered>);

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

pub type BundleId = u64;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Bundle {
    pub id: BundleId,
    pub txs: Vec<TransactionSignedEcRecovered>,
    pub block_num: BlockNumber,
    pub eligibility: RangeInclusive<u64>,
}
