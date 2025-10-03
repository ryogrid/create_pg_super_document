# RemoveScratchTarget

## Location
[src/backend/storage/lmgr/predicate.c:2130-2150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2130-L2150)

## Overview
Removes a dummy entry from the predicate lock target hash table to free up scratch space, as part of PostgreSQL's predicate locking memory management.

## Definition

```c
static void
RemoveScratchTarget(bool lockheld)
```
## Detailed Description
This function removes a special scratch entry from the PredicateLockTargetHash to make room for new entries when the hash table is full. The scratch target is a dummy entry maintained specifically for this purpose - when space is needed, this entry is temporarily removed, allowing new legitimate entries to be inserted.

The function requires the caller to hold SerializablePredicateListLock and expects the caller to restore the scratch entry using RestoreScratchTarget() before releasing the lock. This ensures the scratch space management is atomic and consistent.

The function can optionally manage the partition lock itself, or work with an already-held partition lock for efficiency in scenarios where multiple operations need to be performed under the same lock.

## Parameters / Member Variables
- `lockheld`: A boolean indicating whether the caller already holds the partition lock for the scratch entry's partition
## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md) (assertion check)
  - [LWLockAcquire](../L/LWLockAcquire.md) (conditional)
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - [LWLockRelease](../L/LWLockRelease.md) (conditional)
- Called from (representative examples):
  - [SerialControl](../S/SerialControl.md)
  - [TransferPredicateLocksToNewTarget](../T/TransferPredicateLocksToNewTarget.md)
  - [DropAllPredicateLocksFromTable](../D/DropAllPredicateLocksFromTable.md)

## Notes and Other Information
- Static function, only accessible within predicate.c
- Requires SerializablePredicateListLock to be held by caller (enforced by assertion)
- Must be paired with RestoreScratchTarget() before releasing SerializablePredicateListLock
- Uses ScratchTargetTag and ScratchTargetTagHash global variables
- Part of PostgreSQL's hash table space management for predicate locks
- Conditionally acquires/releases ScratchPartitionLock based on lockheld parameter
- Essential for managing memory pressure in the predicate locking system

## Simplified Source

```c
static void
RemoveScratchTarget(bool lockheld)
{
    bool found;

    Assert(LWLockHeldByMe(SerializablePredicateListLock));

    // Acquire partition lock if not already held
    if (!lockheld)
        LWLockAcquire(ScratchPartitionLock, LW_EXCLUSIVE);

    // Remove the dummy scratch target from hash table
    hash_search_with_hash_value(PredicateLockTargetHash,
                                &ScratchTargetTag,
                                ScratchTargetTagHash,
                                HASH_REMOVE, &found);
    Assert(found);

    // Release partition lock if we acquired it
    if (!lockheld)
        LWLockRelease(ScratchPartitionLock);
}
```