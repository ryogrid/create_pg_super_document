# LockReassignCurrentOwner

## Location
src/backend/storage/lmgr/lock.c: 2569 - 2598

## Overview
LockReassignCurrentOwner reassigns all locks belonging to the current resource owner to its parent resource owner, supporting both optimized array-based and hash table scan modes.

## Definition
```c
void LockReassignCurrentOwner(LOCALLOCK **locallocks, int nlocks)
```

## Detailed Description
This function transfers ownership of locks from the current resource owner to its parent resource owner, which is essential during resource owner cleanup and nested transaction processing. The function provides two execution paths: when provided with a pre-computed array of LOCALLOCK pointers, it processes them efficiently in reverse order; otherwise, it performs a complete hash table scan of LockMethodLocalHash. The function asserts that the current resource owner has a valid parent before proceeding, and delegates the actual ownership transfer to LockReassignOwner for each qualifying lock.

## Parameters / Member Variables
- `locallocks`: Optional array of LOCALLOCK pointers to reassign. If NULL, the function will scan the hash table to find all locks belonging to the current resource owner.
- `nlocks`: Number of locks in the locallocks array. Ignored if locallocks is NULL.

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerGetParent: Retrieves the parent resource owner
  - hash_seq_init: Initializes sequential hash table scanning
  - hash_seq_search: Gets next entry during hash table traversal
  - LockReassignOwner: Performs the actual lock ownership reassignment
  - Assert: Debug assertion macro
- Called from (representative examples):
  - ResourceOwnerReleaseInternal: Called during resource owner cleanup operations
  - LockHashPartitionLockByProc: Referenced in lock management header

## Notes and Other Information
- This function is crucial for maintaining proper lock ownership hierarchies during nested resource owner operations
- The reverse iteration order (i = nlocks - 1; i >= 0; i--) maintains proper processing semantics when an array is provided
- The function asserts that the current resource owner has a parent, as lock reassignment to NULL would be invalid
- Commonly used during subtransaction abort/commit to transfer locks to the parent transaction context
- The optimization of accepting a pre-computed lock array is particularly beneficial for operations like pg_dump that may hold many locks
- Hash table scanning mode provides complete coverage but is more expensive when many locks are present