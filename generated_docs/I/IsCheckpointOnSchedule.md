# IsCheckpointOnSchedule

## Location
[src/backend/postmaster/checkpointer.c:783-861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/checkpointer.c#L783-L861)

## Overview
Determines whether a checkpoint (or restartpoint during recovery) is progressing on schedule to complete within the target time frame based on time elapsed and WAL segments written.

## Definition

```c
struct timeval now;
```
## Detailed Description
This function evaluates checkpoint progress by comparing current advancement against two criteria: time elapsed since checkpoint start and WAL segments written since checkpoint start. It scales the progress according to  and uses caching to avoid expensive calculations when progress hasn't reached the previously calculated target.

The function operates differently during normal operation versus recovery:
- **Normal operation**: Compares current WAL insert location against the location at checkpoint start
- **Recovery mode**: Compares last replayed WAL record location against the location at restartpoint start

The function implements an optimization by caching the elapsed time/segments value to avoid recalculating expensive operations when progress is still below the previously calculated threshold.

## Parameters / Member Variables
- : A double value representing the current checkpoint progress (typically between 0.0 and 1.0)

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [GetXLogReplayRecPtr](../G/GetXLogReplayRecPtr.md)
  - [GetInsertRecPtr](../G/GetInsertRecPtr.md)
  - [gettimeofday](../g/gettimeofday.md)
- Called from (representative examples):
  - [CheckpointWriteDelay](../C/CheckpointWriteDelay.md)

## Notes and Other Information
- The function assumes  is true (verified by Assert)
- Uses global variables: , , , , , , 
- During recovery, the function may allow exceeding  due to the gap between checkpoint redo-pointer and checkpoint record
- The WAL location comparison is an estimate and not completely accurate compared to the actual trigger logic in XLogInsert
- Returns true when checkpoint is on schedule, false otherwise

## Simplified Source

```c
static bool IsCheckpointOnSchedule(double progress)
{
    XLogRecPtr recptr;
    struct timeval now;
    double elapsed_xlogs, elapsed_time;

    Assert(ckpt_active);

    // Scale progress according to completion target
    progress *= CheckPointCompletionTarget;

    // Use cached value to avoid expensive calculations
    if (progress < ckpt_cached_elapsed)
        return false;

    // Check progress against WAL segments written
    if (RecoveryInProgress())
        recptr = GetXLogReplayRecPtr(NULL);
    else
        recptr = GetInsertRecPtr();

    elapsed_xlogs = (((double) (recptr - ckpt_start_recptr)) /
                     wal_segment_size) / CheckPointSegments;

    if (progress < elapsed_xlogs)
    {
        ckpt_cached_elapsed = elapsed_xlogs;
        return false;
    }

    // Check progress against time elapsed
    gettimeofday(&now, NULL);
    elapsed_time = ((double) ((pg_time_t) now.tv_sec - ckpt_start_time) +
                    now.tv_usec / 1000000.0) / CheckPointTimeout;

    if (progress < elapsed_time)
    {
        ckpt_cached_elapsed = elapsed_time;
        return false;
    }

    // Checkpoint is on schedule
    return true;
}
```