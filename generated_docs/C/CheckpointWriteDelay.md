# CheckpointWriteDelay

## Location
[src/backend/postmaster/checkpointer.c:714-782](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/checkpointer.c#L714-L782)

## Overview
Controls the rate of checkpoint writes to achieve the target checkpoint completion time while handling administrative tasks during write delays.

## Definition

```c
struct timeval now;
```
## Detailed Description
CheckpointWriteDelay is called after each page write during BufferSync() operations to throttle the checkpoint write rate. Its primary purpose is to spread checkpoint I/O over time to hit the checkpoint_completion_target, reducing the impact on normal database operations.

The function implements several key behaviors:
1. **Rate Control**: Introduces delays between writes to spread checkpoint I/O over the target duration
2. **Immediate Mode Bypass**: Skips delays when CHECKPOINT_IMMEDIATE flag is set or immediate checkpoints are requested
3. **Administrative Task Processing**: Handles configuration reloads, sync requests, and archive timeouts during delays
4. **Schedule Monitoring**: Uses IsCheckpointOnSchedule() to determine if delays are needed
5. **Fsync Request Management**: Absorbs pending fsync requests periodically to prevent queue overflow

The function only operates when executed by the checkpointer process itself, allowing other processes to call it safely without effect.

## Parameters / Member Variables
- : Checkpoint request flags, particularly CHECKPOINT_IMMEDIATE which disables write delays
- : Estimate of checkpoint completion as a fraction from 0.0 (none) to 1.0 (complete)

## Dependencies
- Functions called/Symbols referenced:
  - AmCheckpointerProcess
  - [ImmediateCheckpointRequested](../I/ImmediateCheckpointRequested.md)
  - [IsCheckpointOnSchedule](../I/IsCheckpointOnSchedule.md)
  - ProcessConfigFile
  - [UpdateSharedMemoryConfig](../U/UpdateSharedMemoryConfig.md)
  - [AbsorbSyncRequests](../A/AbsorbSyncRequests.md)
  - [CheckArchiveTimeout](CheckArchiveTimeout.md)
  - [pgstat_report_checkpointer](../p/pgstat_report_checkpointer.md)
  - [WaitLatch](../W/WaitLatch.md)/ResetLatch
  - [ProcessProcSignalBarrier](../P/ProcessProcSignalBarrier.md)
- Called from (representative examples):
  - [BufferSync](../B/BufferSync.md) (bufmgr.c:3143)

## Notes and Other Information
- Uses WRITES_PER_ABSORB counter to limit fsync request absorption frequency
- Sleep duration is fixed at 100ms (changed from bgwriter_delay connection)
- Bypasses delays during shutdown, immediate checkpoints, or when behind schedule
- Processes configuration reloads and other administrative tasks during write delays
- Includes barrier event processing to maintain process synchronization
- Part of PostgreSQL's I/O smoothing mechanism to reduce checkpoint impact on performance

## Simplified Source

```c
void CheckpointWriteDelay(int flags, double progress)
{
    static int absorb_counter = WRITES_PER_ABSORB;

    // Only execute in checkpointer process
    if (!AmCheckpointerProcess())
        return;

    // Check if we should delay writes (not immediate mode, not shutting down, on schedule)
    if (!(flags & CHECKPOINT_IMMEDIATE) &&
        !ShutdownRequestPending &&
        !ImmediateCheckpointRequested() &&
        IsCheckpointOnSchedule(progress))
    {
        // Handle configuration reload if pending
        if (ConfigReloadPending) {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
            UpdateSharedMemoryConfig();
        }

        // Absorb pending fsync requests
        AbsorbSyncRequests();
        absorb_counter = WRITES_PER_ABSORB;

        // Check for archive timeout
        CheckArchiveTimeout();

        // Report statistics
        pgstat_report_checkpointer();

        // Sleep for 100ms to throttle write rate
        WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
                  100, WAIT_EVENT_CHECKPOINT_WRITE_DELAY);
        ResetLatch(MyLatch);
    }
    else if (--absorb_counter <= 0) {
        // Even when not sleeping, periodically absorb fsync requests
        // to prevent queue overflow
        AbsorbSyncRequests();
        absorb_counter = WRITES_PER_ABSORB;
    }

    // Handle barrier events if pending
    if (ProcSignalBarrierPending)
        ProcessProcSignalBarrier();
}
```