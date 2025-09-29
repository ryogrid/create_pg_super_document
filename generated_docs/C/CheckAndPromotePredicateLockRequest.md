# CheckAndPromotePredicateLockRequest

## Location
[src/backend/storage/lmgr/predicate.c:2316-2380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2316-L2380)

## Overview
Checks all ancestors of a newly-acquired predicate lock, increments their child counts, and promotes to a coarser lock if any ancestor exceeds its promotion threshold.

## Definition

```c
static bool
CheckAndPromotePredicateLockRequest(const PREDICATELOCKTARGETTAG *reqtag)
```
## Detailed Description
CheckAndPromotePredicateLockRequest is a static function in PostgreSQL's predicate locking system that implements lock promotion logic. When a new predicate lock is acquired, this function traverses up the lock hierarchy to examine all ancestor locks (e.g., from tuple to page to relation). For each ancestor, it increments the child lock count in the local predicate lock hash table.

If any ancestor's child count exceeds the threshold returned by MaxPredicateChildLocks(), the function identifies that ancestor as a candidate for promotion. The function continues traversing ancestors to ensure accurate child counts and to find the coarsest (most general) lock that should be promoted to, optimizing for the most efficient lock granularity.

When promotion is determined necessary, the function acquires the coarsest eligible ancestor lock, which automatically triggers deletion of redundant child locks. This mechanism prevents lock proliferation and maintains efficient memory usage while preserving correctness in serializable snapshot isolation.

## Parameters / Member Variables
- : Pointer to a PREDICATELOCKTARGETTAG representing the newly requested lock target for which ancestors should be checked for promotion opportunities.

## Dependencies
- Functions called/Symbols referenced:
  - [GetParentPredicateLockTag](../G/GetParentPredicateLockTag.md)
  - [hash_search](../h/hash_search.md)
  - [MaxPredicateChildLocks](../M/MaxPredicateChildLocks.md)
  - [PredicateLockAcquire](../P/PredicateLockAcquire.md)
  - HASH_ENTER
  - [PREDICATELOCKTARGETTAG](../P/PREDICATELOCKTARGETTAG.md) (data structure)
  - [LOCALPREDICATELOCK](../L/LOCALPREDICATELOCK.md) (data structure)
- Called from (representative examples):
  - [SerialControl](../S/SerialControl.md)
  - [PredicateLockAcquire](../P/PredicateLockAcquire.md)

## Notes and Other Information
- This is a static function only accessible within the predicate.c file
- Returns true if a parent lock was acquired (promotion occurred), false otherwise
- The function maintains child lock counts in LocalPredicateLockHash for promotion decisions
- Uses iterative traversal up the lock hierarchy to examine all ancestors
- Promotes to the coarsest ancestor that exceeds its threshold, not just the first one found
- The promotion mechanism is essential for preventing memory exhaustion from too many fine-grained locks
- Part of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation
- The function ensures that parent lock counts are accurate even when promotion occurs
- Lock promotion automatically triggers cleanup of redundant child locks through PredicateLockAcquire
- Critical for maintaining performance in workloads with many small locks

## Simplified Source

```c
static bool
CheckAndPromotePredicateLockRequest(const PREDICATELOCKTARGETTAG *reqtag)
{
    PREDICATELOCKTARGETTAG current_tag, parent_tag, promotion_tag;
    LOCALPREDICATELOCK *parent_lock;
    bool should_promote = false;

    current_tag = *reqtag;

    // Walk up the lock hierarchy checking each ancestor
    while (GetParentPredicateLockTag(&current_tag, &parent_tag))
    {
        current_tag = parent_tag;

        // Find or create parent lock entry in hash table
        parent_lock = hash_search(LocalPredicateLockHash, &current_tag, HASH_ENTER, &found);

        if (!found) {
            // New entry - initialize with first child
            parent_lock->held = false;
            parent_lock->childLocks = 1;
        } else {
            // Existing entry - increment child count
            parent_lock->childLocks++;
        }

        // Check if this parent has too many children
        if (parent_lock->childLocks > MaxPredicateChildLocks(&current_tag)) {
            // Mark for promotion but keep checking ancestors
            // to find the coarsest eligible lock
            promotion_tag = current_tag;
            should_promote = true;
        }
    }

    if (should_promote) {
        // Acquire the coarsest ancestor that exceeded threshold
        PredicateLockAcquire(&promotion_tag);
        return true;
    }

    return false;
}
```