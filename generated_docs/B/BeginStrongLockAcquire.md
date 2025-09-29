# BeginStrongLockAcquire

## Location
[src/backend/storage/lmgr/lock.c:1724-1749](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1724-L1749)

## Overview
BeginStrongLockAcquire inhibits the use of the fast-path locking mechanism for a given LOCALLOCK and sets up error cleanup handling for strong lock acquisition.

## Definition
```c
static void BeginStrongLockAcquire(LOCALLOCK *locallock, uint32 fasthashcode)
```

## Detailed Description
BeginStrongLockAcquire is part of PostgreSQL's lock management system that handles the transition from fast-path locking to the full lock table mechanism. When a lock cannot be handled through the fast-path (due to conflicts or other constraints), this function prepares the system for a "strong" lock acquisition by updating global counters and marking the local lock appropriately.

The function increments a count in the FastPathStrongRelationLocks structure for the given hash code, which prevents other backends from using the fast-path for locks that hash to the same value. It uses spinlock protection to ensure atomic updates to the count. The function also sets up global state (StrongLockInProgress) to enable proper cleanup if the strong lock acquisition fails.

## Parameters / Member Variables
- `locallock`: Pointer to the LOCALLOCK structure that will undergo strong lock acquisition
- `fasthashcode`: Hash code used to identify the slot in the fast-path strong lock count array

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
- Global variables used:
  - StrongLockInProgress
  - FastPathStrongRelationLocks
- Data structures used:
  - [LOCALLOCK](../L/LOCALLOCK.md)
- Called from (representative examples):
  - [LockAcquireExtended](../L/LockAcquireExtended.md)

## Notes and Other Information
- This is a static function only accessible within lock.c
- The function includes assertions to ensure it's not called when another strong lock is already in progress
- Uses spinlock protection for thread-safe updates to the global count array
- Comments suggest that atomic fetch-and-add instructions could be considered as an optimization
- The holdsStrongLockCount flag is set to true to track that this LOCALLOCK holds a strong lock count
- Must be paired with either FinishStrongLockAcquire or AbortStrongLockAcquire for proper cleanup

## Simplified Source

```c
// Simplified version of BeginStrongLockAcquire
static void BeginStrongLockAcquire(LOCALLOCK *locallock, uint32 fasthashcode) {
    // Verify preconditions - no other strong lock in progress
    Assert(StrongLockInProgress == NULL);
    Assert(locallock->holdsStrongLockCount == false);

    // Atomically increment the strong lock count for this hash code
    // Uses spinlock to prevent race conditions with other backends
    SpinLockAcquire(&FastPathStrongRelationLocks->mutex);
    FastPathStrongRelationLocks->count[fasthashcode]++;

    // Mark this locallock as holding a strong lock count
    locallock->holdsStrongLockCount = true;

    // Set global state for error cleanup handling
    StrongLockInProgress = locallock;

    SpinLockRelease(&FastPathStrongRelationLocks->mutex);
}
```

Key simplifications made:
- Removed detailed comments about atomic operations considerations
- Consolidated the core logic into clear sequential steps
- Added high-level comments explaining the purpose of each operation
- Preserved all essential functionality and assertions
- Maintained the spinlock protection pattern for thread safety