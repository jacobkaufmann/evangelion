use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use super::{Bundle, BundleId};

use reth_primitives::BlockNumber;
use reth_provider::CanonStateNotification;

#[derive(Default)]
pub struct BundlePool(pub(crate) HashSet<Bundle>);

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
