use crate::error::Result;
use crate::model::Metrics;
use crate::policy::Policy;
use crate::store::Store;

/// Fills group, participant and space metrics from the verified groups.
///
/// `logical_redundant_bytes` counts the logical length of every eligible copy.
/// `estimated_allocated_bytes` counts allocated blocks only for eligible copies with a single
/// link; copies with other hard links free nothing when one name is removed. Neither number
/// is a promise of physical reclamation: shared extents, compression, snapshots and open
/// descriptors can all keep blocks alive.
pub fn finish(store: &Store, policy: &Policy, metrics: &mut Metrics) -> Result<()> {
    store.for_each_group(|group| {
        let (keeper, _) = policy.select_streaming(store, group)?;
        metrics.groups += 1;
        store.for_each_member(group, |member| {
            metrics.participants += 1;
            if member.id != keeper.id && !policy.is_protected(&member.path) {
                metrics.eligible_copies += 1;
                metrics.logical_redundant_bytes += member.observation.size;
                if member.observation.links == 1 {
                    metrics.estimated_allocated_bytes += member.observation.allocated;
                }
            }
            Ok(())
        })
    })
}
