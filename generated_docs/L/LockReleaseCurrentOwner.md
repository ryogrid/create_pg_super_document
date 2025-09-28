# LockReleaseCurrentOwner

## Location
[src/backend/storage/lmgr/lock.c:2474-2508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L2474-L2508)

## Overview
LockReleaseCurrentOwner releases all locks belonging to the current resource owner, with an optimization to accept a pre-computed array of locks to avoid hash table traversal.

## Definition
```c
void LockReleaseCurrentOwner(LOCALLOCK **locallocks, int nlocks)
```

## Detailed Description
This function releases all locks held by the current resource owner. It provides two execution paths for performance optimization: if the caller provides an array of LOCALLOCK pointers, the function iterates through them in reverse order (to maintain proper release semantics); otherwise, it performs a full hash table scan of LockMethodLocalHash to find all relevant locks. In both cases, ReleaseLockIfHeld is called with the session flag set to false, indicating these are transaction-level locks rather than session-level locks.

## Parameters / Member Variables
- `locallocks`: Optional array of LOCALLOCK pointers to release. If NULL, the function will scan the hash table to find all locks belonging to the current resource owner.
- `nlocks`: Number of locks in the locallocks array. Ignored if locallocks is NULL.

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md): Initializes sequential hash table scanning
  - [hash_seq_search](../h/hash_seq_search.md): Gets next entry during hash table traversal
  - [ReleaseLockIfHeld](../R/ReleaseLockIfHeld.md): Releases a specific lock if it is held by the current resource owner
- Called from (representative examples):
  - [ResourceOwnerReleaseInternal](../R/ResourceOwnerReleaseInternal.md): Called during resource owner cleanup
  - LockHashPartitionLockByProc: Referenced in lock management header

## Notes and Other Information
- The function processes locks in reverse order when an array is provided, which helps maintain proper lock release semantics and dependencies
- When locallocks is NULL, the function performs a complete scan of the local lock hash table, which can be expensive if many locks are held
- The false parameter passed to ReleaseLockIfHeld indicates these are transaction-level locks, not session-level locks
- This function is primarily used during transaction abort or commit processing to ensure proper lock cleanup
- The optimization of passing a pre-computed lock array is particularly beneficial when a large number of locks are held

## Simplified Source

```c
// Simplified version of LockReleaseCurrentOwner
void LockReleaseCurrentOwner(LOCALLOCK **locallocks, int nlocks) {
    // Two approaches: use provided lock array or scan hash table
    if (locallocks == NULL) {
        // Approach 1: Scan all locks in hash table
        HASH_SEQ_STATUS status;
        LOCALLOCK *locallock;

        hash_seq_init(&status, LockMethodLocalHash);
        while ((locallock = hash_seq_search(&status)) != NULL) {
            ReleaseLockIfHeld(locallock, false);
        }
    } else {
        // Approach 2: Release locks from provided array (reverse order)
        for (int i = nlocks - 1; i >= 0; i--) {
            ReleaseLockIfHeld(locallocks[i], false);
        }
    }
}
```

Key simplifications made:
- Removed detailed comments and kept essential logic flow clear
- Simplified variable declarations and moved them closer to usage
- Made the two-path approach more obvious with clear comments
- Consolidated the core logic into clean, readable blocks
- Preserved the reverse-order iteration which is important for lock semantics