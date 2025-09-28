# UnregisterSnapshotNoOwner

## Location
[src/backend/utils/time/snapmgr.c:859-879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L859-L879)

## Overview
Core function that decrements a snapshot's reference count, removes it from the RegisteredSnapshots pairing heap when appropriate, and frees the snapshot when no references remain.

## Definition
```c
static void UnregisterSnapshotNoOwner(Snapshot snapshot)
```

## Detailed Description
UnregisterSnapshotNoOwner is the core implementation of snapshot unregistration that handles reference count management and memory cleanup. It decrements the snapshot's regd_count, removes the snapshot from the RegisteredSnapshots pairing heap when the reference count reaches zero, and frees the snapshot memory when both regd_count and active_count reach zero. The function includes assertions to ensure proper state and pairing heap consistency. After freeing a snapshot, it calls SnapshotResetXmin to potentially update the global transaction visibility state.

## Parameters / Member Variables
- `snapshot`: The snapshot to unregister. Must be a valid snapshot with regd_count > 0.

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_is_empty
  - [pairingheap_remove](../p/pairingheap_remove.md)
  - [FreeSnapshot](../F/FreeSnapshot.md)
  - [SnapshotResetXmin](../S/SnapshotResetXmin.md)

## Simplified Source

```c
// Simplified version of UnregisterSnapshotNoOwner
static void UnregisterSnapshotNoOwner(Snapshot snapshot) {
    // Validate state
    Assert(snapshot->regd_count > 0);
    Assert(!pairingheap_is_empty(&RegisteredSnapshots));

    // Decrement reference count
    snapshot->regd_count--;

    // Remove from registered snapshots heap when ref count reaches zero
    if (snapshot->regd_count == 0)
        pairingheap_remove(&RegisteredSnapshots, &snapshot->ph_node);

    // Free snapshot if no references remain
    if (snapshot->regd_count == 0 && snapshot->active_count == 0) {
        FreeSnapshot(snapshot);
        SnapshotResetXmin();
    }
}
```

Key simplifications made:
- Focused on the reference counting and cleanup logic
- Emphasized the two-stage cleanup (remove from heap, then free)
- Added clear comments for each validation and cleanup step
- Simplified the conditional logic structure
- Called from (representative examples):
  - [UnregisterSnapshotFromOwner](UnregisterSnapshotFromOwner.md)
  - [ResOwnerReleaseSnapshot](../R/ResOwnerReleaseSnapshot.md)

## Notes and Other Information
- Static function, only called internally within snapmgr.c
- Asserts that regd_count > 0 and RegisteredSnapshots heap is not empty
- Removes from pairing heap only when regd_count reaches zero
- Frees snapshot memory only when both regd_count and active_count are zero
- Calls SnapshotResetXmin after freeing to update global visibility state
- Critical for preventing memory leaks in snapshot management
- Located in src/backend/utils/time/snapmgr.c:859-879