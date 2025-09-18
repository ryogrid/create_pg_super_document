# PerformWalRecovery

## Location
src/backend/access/transam/xlogrecovery.c: 1652 - 1907

## Overview
PerformWalRecovery performs WAL (Write-Ahead Log) recovery by replaying WAL records from the REDO start location to either the end of available WAL or a configured recovery target.

## Definition


## Detailed Description
PerformWalRecovery is the main function responsible for WAL recovery in PostgreSQL. It is called during server startup when the system was not shut down cleanly. The function performs the following key operations:

1. **Initialization**: Sets up shared variables for tracking WAL replay progress and initializes recovery state
2. **Recovery Start**: Signals the postmaster that recovery has started and checks for immediate consistency 
3. **Record Location**: Finds the first WAL record that logically follows the checkpoint, either at the REDO start LSN or after the checkpoint location
4. **Main Recovery Loop**: Iterates through WAL records, applying each one via ApplyWalRecord until reaching the end of WAL or a recovery target
5. **Recovery Completion**: Handles different recovery target actions (shutdown, pause, promote) and performs cleanup

The function handles various recovery scenarios including:
- Recovery pause requests from hot-standby sessions
- Recovery delay configuration for lagging behind the primary
- Different recovery target actions based on configuration
- Progress reporting for non-standby modes

## Parameters / Member Variables
This function takes no parameters as it operates on global recovery state.

## Dependencies
- Functions called/Symbols referenced:
  - [CheckRecoveryConsistency](../C/CheckRecoveryConsistency.md)
  - [ApplyWalRecord](../A/ApplyWalRecord.md)
  - [ReadRecord](../R/ReadRecord.md)
  - [XLogPrefetcherBeginRead](../X/XLogPrefetcherBeginRead.md)
  - [HandleStartupProcInterrupts](../H/HandleStartupProcInterrupts.md)
  - [recoveryStopsBefore](../r/recoveryStopsBefore.md)/recoveryStopsAfter
  - [recoveryApplyDelay](../r/recoveryApplyDelay.md)
  - [recoveryPausesHere](../r/recoveryPausesHere.md)
  - [RmgrStartup](../R/RmgrStartup.md)/RmgrCleanup
  - SendPostmasterSignal
  - [SetRecoveryPause](../S/SetRecoveryPause.md)
- Called from:
  - [StartupXLOG](../S/StartupXLOG.md) (src/backend/access/transam/xlog.c:5803)

## Notes and Other Information
- This function is only called when the system was not shut down cleanly
- It maintains detailed progress tracking via XLogRecoveryCtl shared memory structure
- The function supports different recovery targets: time, LSN, transaction ID, or named restore points
- WAL prefetching is used to improve I/O performance during recovery
- Recovery can be paused and resumed via hot-standby session requests
- The function logs detailed information about recovery start, progress, and completion
- Resource manager startup and cleanup are handled at the beginning and end of recovery