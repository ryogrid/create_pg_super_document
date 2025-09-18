# ParallelWorkerReportLastRecEnd

## Location
src/backend/access/transam/parallel.c: 1573 - 1600

## Overview
ParallelWorkerReportLastRecEnd updates shared memory with the ending location of the last WAL record written by a parallel worker, maintaining the maximum value across all workers.

## Definition
```c
void ParallelWorkerReportLastRecEnd(XLogRecPtr last_xlog_end)
```

## Detailed Description
ParallelWorkerReportLastRecEnd is a thread-safe function that allows parallel workers to report their WAL (Write-Ahead Log) write progress to the parallel leader process. The function uses spinlock-protected shared memory to maintain the highest WAL end position seen across all parallel workers. This information is crucial for WAL consistency and recovery operations in parallel query execution.

The function performs an atomic compare-and-update operation: it only updates the shared `last_xlog_end` field if the provided value is greater than the current stored value. This ensures that the shared state always reflects the furthest point in the WAL that any parallel worker has written to.

The shared state is maintained in the `FixedParallelState` structure, which is accessible to all workers in the parallel group through the `MyFixedParallelState` global variable.

## Parameters / Member Variables
- `last_xlog_end`: An XLogRecPtr indicating the ending location of the last WAL record written by this parallel worker

## Dependencies
- Functions called/Symbols referenced:
  - [FixedParallelState](../F/FixedParallelState.md) (struct type)
  - SpinLockAcquire, SpinLockRelease (spinlock operations)
  - MyFixedParallelState (global variable)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md) (during transaction commit)
  - IsParallelWorker (helper function)

## Notes and Other Information
- Thread-safe implementation using spinlocks to protect shared memory access
- Only updates the shared value if the new value is greater (monotonic increase)
- Requires that `MyFixedParallelState` is not NULL (enforced by assertion)
- Part of the parallel worker WAL coordination mechanism
- Critical for maintaining WAL consistency across parallel operations
- The `mutex` field in `FixedParallelState` provides the necessary synchronization