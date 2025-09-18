# CheckpointWriteDelay

## Location
src/backend/postmaster/checkpointer.c: 714 - 782

## Overview
Controls the rate of checkpoint writes to achieve the target checkpoint completion time while handling administrative tasks during write delays.

## Definition


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
  - ImmediateCheckpointRequested
  - IsCheckpointOnSchedule
  - ProcessConfigFile
  - UpdateSharedMemoryConfig
  - AbsorbSyncRequests
  - CheckArchiveTimeout
  - pgstat_report_checkpointer
  - WaitLatch/ResetLatch
  - ProcessProcSignalBarrier
- Called from (representative examples):
  - BufferSync (bufmgr.c:3143)

## Notes and Other Information
- Uses WRITES_PER_ABSORB counter to limit fsync request absorption frequency
- Sleep duration is fixed at 100ms (changed from bgwriter_delay connection)
- Bypasses delays during shutdown, immediate checkpoints, or when behind schedule
- Processes configuration reloads and other administrative tasks during write delays
- Includes barrier event processing to maintain process synchronization
- Part of PostgreSQL's I/O smoothing mechanism to reduce checkpoint impact on performance