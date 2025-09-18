# DecrementParentLocks

## Location
src/backend/storage/lmgr/predicate.c: 2381 - 2442

## Overview
Decrements the child count on all ancestor locks when releasing a predicate lock that becomes redundant or unnecessary.

## Definition
static void DecrementParentLocks(const PREDICATELOCKTARGETTAG *targettag)

## Detailed Description
This function is responsible for managing the hierarchical relationship between predicate locks by decrementing the child count on all ancestor locks when a child lock is released. It's specifically called when releasing a lock via DeleteChildTargetLocks (when a lock becomes redundant because its parent has been acquired, possibly due to promotion) or when a new MVCC write lock makes the predicate lock unnecessary.

The function walks up the lock hierarchy starting from the given target tag, finding each parent lock and decrementing its child count. If a parent lock has no children and is not held, it removes the lock entry from the hash table. The function includes safety checks to handle edge cases where parent locks might not exist or have negative child counts due to index splits.

## Parameters / Member Variables
- : Pointer to the predicate lock target tag whose parent locks need their child counts decremented

## Dependencies
- Functions called/Symbols referenced:
  - GetParentPredicateLockTag
  - PredicateLockTargetTagHashCode
  - hash_search_with_hash_value
  - PREDICATELOCKTARGETTAG (struct)
  - LOCALPREDICATELOCK (struct)
  - HASH_FIND, HASH_REMOVE (constants)
- Called from (representative examples):
  - DeleteChildTargetLocks
  - CheckTargetForConflictsIn
  - SerialControl

## Notes and Other Information
- This function is only called during lock release operations, not at transaction end when the information is no longer needed
- Includes defensive programming with assertions to handle cases where parent locks might be missing due to index splits
- The function handles edge cases where parent lock refcounts might be zero or negative
- Uses hash table operations for efficient parent lock lookup and removal