# RemoveLocalLock

## Location
[src/backend/storage/lmgr/lock.c:1376-1428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1376-L1428)

## Overview
RemoveLocalLock is a static function that completely cleans up and removes a LOCALLOCK entry from the local lock hash table when a lock is no longer needed.

## Definition
```c
static void RemoveLocalLock(LOCALLOCK *locallock)
```

## Detailed Description
This function performs complete cleanup of a LOCALLOCK structure when it is being removed from the system. It handles several critical cleanup tasks:

1. **Resource Owner Cleanup**: Iterates through all lock owners and notifies each resource owner that the lock is being forgotten via ResourceOwnerForgetLock()
2. **Memory Management**: Frees the lockOwners array and resets the numLockOwners counter
3. **Fast Path Strong Lock Management**: If the lock was holding a strong lock count, decrements the appropriate counter in the FastPathStrongRelationLocks structure under spinlock protection
4. **Hash Table Removal**: Removes the LOCALLOCK entry from the LockMethodLocalHash table
5. **Lock Status Update**: Calls CheckAndSetLockHeld() to update lock status tracking

The function is designed to be called as a final cleanup step when a LOCALLOCK is being completely removed, ensuring no references or resources are leaked.

## Parameters / Member Variables
- `locallock`: Pointer to the LOCALLOCK structure to be removed and cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerForgetLock](ResourceOwnerForgetLock.md)
  - FastPathStrongLockHashPartition
  - [hash_search](../h/hash_search.md)
  - [CheckAndSetLockHeld](../C/CheckAndSetLockHeld.md)
  - HASH_REMOVE (constant)
- Called from (representative examples):
  - [LockAcquireExtended](../L/LockAcquireExtended.md)
  - [LockRelease](../L/LockRelease.md)
  - [LockReleaseAll](../L/LockReleaseAll.md)
  - [PostPrepare_Locks](../P/PostPrepare_Locks.md)

## Notes and Other Information
- This is a static function internal to lock.c, not exposed to external modules
- The function handles both regular locks and fast-path strong locks appropriately
- Memory cleanup is thorough, preventing leaks in the lockOwners array
- The function uses spinlocks when updating shared fast-path lock counters
- Error handling includes a warning if the local lock hash table appears corrupted
- The function assumes the caller has already handled any necessary unlocking of the actual lock object

## Simplified Source

```c
// Simplified version of RemoveLocalLock
static void RemoveLocalLock(LOCALLOCK *locallock)
{
    // Step 1: Clean up all resource owners
    for (int i = locallock->numLockOwners - 1; i >= 0; i--) {
        if (locallock->lockOwners[i].owner != NULL) {
            ResourceOwnerForgetLock(locallock->lockOwners[i].owner, locallock);
        }
    }

    // Step 2: Free lock owners array and reset count
    locallock->numLockOwners = 0;
    if (locallock->lockOwners != NULL) {
        pfree(locallock->lockOwners);
        locallock->lockOwners = NULL;
    }

    // Step 3: Handle fast-path strong lock cleanup if needed
    if (locallock->holdsStrongLockCount) {
        uint32 partition = FastPathStrongLockHashPartition(locallock->hashcode);

        SpinLockAcquire(&FastPathStrongRelationLocks->mutex);
        FastPathStrongRelationLocks->count[partition]--;
        locallock->holdsStrongLockCount = false;
        SpinLockRelease(&FastPathStrongRelationLocks->mutex);
    }

    // Step 4: Remove from local lock hash table
    if (!hash_search(LockMethodLocalHash, &(locallock->tag), HASH_REMOVE, NULL)) {
        elog(WARNING, "locallock table corrupted");
    }

    // Step 5: Update lock status tracking
    CheckAndSetLockHeld(locallock, false);
}
```

Key simplifications made:
- Simplified loop variable declaration to modern C style
- Added descriptive comments for each major cleanup step
- Consolidated the fast-path logic flow for better readability
- Retained all essential error checking and resource cleanup
- Preserved the exact order of operations which is critical for correctness
- Kept all memory management and synchronization primitives intact