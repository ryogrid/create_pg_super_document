# DeleteChildTargetLocks

## Location
[src/backend/storage/lmgr/predicate.c:2204-2278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2204-L2278)

## Overview
Deletes child target locks owned by the current process that are covered by a new target, implementing lock promotion optimization in the predicate locking system.

## Definition

```c
static void
DeleteChildTargetLocks(const PREDICATELOCKTARGETTAG *newtargettag)
```
## Detailed Description
DeleteChildTargetLocks is a static function in PostgreSQL's predicate locking system that removes child (more granular) predicate locks when a parent (less granular) lock is acquired. This implements lock promotion, where acquiring a coarser lock (like a page lock) makes finer locks (like tuple locks) redundant. The function iterates through all predicate locks held by the current serializable transaction and removes any locks whose targets are covered by the new target.

The function handles both normal and parallel execution modes, acquiring appropriate locks to ensure thread safety. In parallel mode, it acquires the per-transaction predicate list lock to coordinate with worker processes. For each lock that should be removed, it properly cleans up all associated data structures including removing entries from hash tables and linked lists.

This optimization is crucial for performance, preventing the accumulation of redundant fine-grained locks when coarser locks are sufficient, thereby reducing memory usage and lock checking overhead.

## Parameters / Member Variables
- : Pointer to the PREDICATELOCKTARGETTAG representing the new (typically coarser) lock target. Child locks whose targets are covered by this new target will be deleted.

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [IsInParallelMode](../I/IsInParallelMode.md)
  - dlist_foreach_modify
  - dlist_container
  - TargetTagIsCoveredBy
  - PredicateLockTargetTagHashCode
  - PredicateLockHashPartitionLock
  - [dlist_delete](../d/dlist_delete.md)
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - PredicateLockHashCodeFromTargetHashCode
  - [RemoveTargetIfNoLongerUsed](../R/RemoveTargetIfNoLongerUsed.md)
  - [DecrementParentLocks](DecrementParentLocks.md)
  - [SERIALIZABLEXACT](../S/SERIALIZABLEXACT.md), PREDICATELOCK (data structures)
- Called from (representative examples):
  - [SerialControl](../S/SerialControl.md)
  - [PredicateLockAcquire](../P/PredicateLockAcquire.md)

## Notes and Other Information
- This is a static function only accessible within the predicate.c file
- Assumes uniform usage of target tag fields for optimization purposes
- Handles parallel mode execution by acquiring additional locks for thread safety
- The function implements lock promotion optimization to reduce memory overhead
- Properly maintains all linked list and hash table invariants during lock removal
- Uses PG_USED_FOR_ASSERTS_ONLY to avoid unused variable warnings in non-debug builds
- Part of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation
- The function only operates on locks owned by the current transaction (MySerializableXact)
- Careful lock ordering prevents deadlocks during cleanup operations