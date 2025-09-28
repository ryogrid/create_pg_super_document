# WALInsertLockAcquire

## Location
[src/backend/access/transam/xlog.c:1373-1417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L1373-L1417)

## Overview
WALInsertLockAcquire acquires one of the available WAL insertion locks to coordinate concurrent WAL record insertions, using an affinity-based selection strategy to minimize cache line bouncing.

## Definition
```c
static void WALInsertLockAcquire(void)
```

## Detailed Description
WALInsertLockAcquire implements a smart lock acquisition strategy for WAL insertion that balances performance and fairness. It first attempts to acquire the same lock used in the previous insertion to maintain cache affinity and avoid unnecessary cache line bouncing between processes when there's low contention. For new backends, it selects a lock semi-randomly based on the process number to ensure even distribution across available locks. If the preferred lock cannot be acquired immediately, the function implements adaptive behavior by trying the next lock in sequence for subsequent attempts, which helps distribute load evenly across all available insertion locks.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - NUM_XLOGINSERT_LOCKS (constant)
  - MyProcNumber (global variable)
  - MyLockNo (global variable)
  - WALInsertLocks (global array)
- Called from (representative examples):
  - [XLogInsertRecord](../X/XLogInsertRecord.md)
  - [CreateOverwriteContrecordRecord](../C/CreateOverwriteContrecordRecord.md)
  - RefreshXLogWriteResult

## Notes and Other Information
- Uses a static variable lockToTry to remember the preferred lock number across calls
- For first-time backends, initializes lockToTry using MyProcNumber % NUM_XLOGINSERT_LOCKS for even distribution
- Implements adaptive lock selection: if immediate acquisition fails, tries the next lock in sequence next time
- The insertingAt value in the acquired lock is initially set to 0 since the insert location is not yet known
- Acquires locks in LW_EXCLUSIVE mode to ensure exclusive access during insertion
- The strategy optimizes for both low-contention scenarios (cache affinity) and high-contention scenarios (load distribution)
- Works with a fixed number of insertion locks (NUM_XLOGINSERT_LOCKS) that is typically much smaller than the number of potential concurrent inserters

## Simplified Source

```c
// Simplified version of WALInsertLockAcquire
static void WALInsertLockAcquire(void) {
    static int lockToTry = -1;
    bool immed;

    // Initialize lock selection for first-time backends
    // Use process number to distribute locks evenly across backends
    if (lockToTry == -1) {
        lockToTry = MyProcNumber % NUM_XLOGINSERT_LOCKS;
    }

    // Try to acquire the preferred lock (cache affinity optimization)
    MyLockNo = lockToTry;
    immed = LWLockAcquire(&WALInsertLocks[MyLockNo].l.lock, LW_EXCLUSIVE);

    // Adaptive behavior: if lock wasn't immediately available,
    // try next lock in sequence for future attempts
    if (!immed) {
        lockToTry = (lockToTry + 1) % NUM_XLOGINSERT_LOCKS;
    }
}
```

Key simplifications made:
- Consolidated variable declarations for clarity
- Simplified comments to focus on core logic flow
- Removed detailed commentary while preserving essential algorithm
- Maintained the two key optimizations: cache affinity and adaptive load distribution
- Preserved the static variable behavior and modular arithmetic for lock cycling