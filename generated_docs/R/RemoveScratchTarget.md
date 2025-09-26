# RemoveScratchTarget

## Location
src/backend/storage/lmgr/predicate.c: 2130 - 2150

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
- : A boolean indicating whether the caller already holds the partition lock for the scratch entry's partition

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMe (assertion check)
  - LWLockAcquire (conditional)
  - hash_search_with_hash_value
  - LWLockRelease (conditional)
- Called from (representative examples):
  - SerialControl
  - TransferPredicateLocksToNewTarget
  - DropAllPredicateLocksFromTable

## Notes and Other Information
- Static function, only accessible within predicate.c
- Requires SerializablePredicateListLock to be held by caller (enforced by assertion)
- Must be paired with RestoreScratchTarget() before releasing SerializablePredicateListLock
- Uses ScratchTargetTag and ScratchTargetTagHash global variables
- Part of PostgreSQL's hash table space management for predicate locks
- Conditionally acquires/releases ScratchPartitionLock based on lockheld parameter
- Essential for managing memory pressure in the predicate locking system