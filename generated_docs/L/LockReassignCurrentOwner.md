# LockReassignCurrentOwner

## Location
[src/backend/storage/lmgr/lock.c:2569-2598](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L2569-L2598)

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
  - [ResourceOwnerGetParent](../R/ResourceOwnerGetParent.md): Retrieves the parent resource owner
  - [hash_seq_init](../h/hash_seq_init.md): Initializes sequential hash table scanning
  - [hash_seq_search](../h/hash_seq_search.md): Gets next entry during hash table traversal
  - [LockReassignOwner](LockReassignOwner.md): Performs the actual lock ownership reassignment
  - Assert: Debug assertion macro
- Called from (representative examples):
  - [ResourceOwnerReleaseInternal](../R/ResourceOwnerReleaseInternal.md): Called during resource owner cleanup operations
  - LockHashPartitionLockByProc: Referenced in lock management header

## Notes and Other Information
- This function is crucial for maintaining proper lock ownership hierarchies during nested resource owner operations
- The reverse iteration order (i = nlocks - 1; i >= 0; i--) maintains proper processing semantics when an array is provided
- The function asserts that the current resource owner has a parent, as lock reassignment to NULL would be invalid
- Commonly used during subtransaction abort/commit to transfer locks to the parent transaction context
- The optimization of accepting a pre-computed lock array is particularly beneficial for operations like pg_dump that may hold many locks
- [Hash](../H/Hash.md) table scanning mode provides complete coverage but is more expensive when many locks are present

## Simplified Source

```c
// Simplified version of LockReassignCurrentOwner
void LockReassignCurrentOwner(LOCALLOCK **locallocks, int nlocks) {
    // Get the parent resource owner for lock reassignment
    ResourceOwner parent = ResourceOwnerGetParent(CurrentResourceOwner);

    // Ensure we have a valid parent to transfer locks to
    Assert(parent != NULL);

    if (locallocks == NULL) {
        // No array provided - scan all locks in hash table
        HASH_SEQ_STATUS status;
        LOCALLOCK *locallock;

        // Initialize hash table scan
        hash_seq_init(&status, LockMethodLocalHash);

        // Process each lock found in the hash table
        while ((locallock = hash_seq_search(&status)) != NULL) {
            LockReassignOwner(locallock, parent);
        }
    } else {
        // Array provided - process locks in reverse order
        for (int i = nlocks - 1; i >= 0; i--) {
            LockReassignOwner(locallocks[i], parent);
        }
    }
}
```

Key simplifications made:
- Simplified variable declarations and combined them where appropriate
- Added clear comments explaining the two execution paths
- Maintained the essential logic flow: get parent, validate, then either scan hash table or iterate array
- Preserved the reverse iteration order for the array case
- Removed detailed type casting for clarity while maintaining correctness
- Consolidated the core algorithm into clearly commented sections