# PerformWalRecovery

## Location
[src/backend/access/transam/xlogrecovery.c:1652-1907](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L1652-L1907)

## Overview
PerformWalRecovery performs WAL (Write-Ahead Log) recovery by replaying WAL records from the REDO start location to either the end of available WAL or a configured recovery target.

## Definition

```c
void
PerformWalRecovery(void)
```
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
  - [SendPostmasterSignal](../S/SendPostmasterSignal.md)
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

## Simplified Source

```c
// Simplified version of PerformWalRecovery
void PerformWalRecovery(void) {
    XLogRecord *record;
    bool reachedRecoveryTarget = false;
    TimeLineID replayTLI;

    // Initialize recovery progress tracking
    InitializeRecoveryProgress();

    // Signal postmaster that recovery has started
    if (IsUnderPostmaster)
        SendPostmasterSignal(PMSIGNAL_RECOVERY_STARTED);

    // Check if we can allow read-only connections
    CheckRecoveryConsistency();

    // Find first record to replay
    if (RedoStartLSN < CheckPointLoc) {
        // Start from REDO location
        replayTLI = RedoStartTLI;
        XLogPrefetcherBeginRead(xlogprefetcher, RedoStartLSN);
        record = ReadRecord(xlogprefetcher, PANIC, false, replayTLI);

        // Verify it's a checkpoint redo record
        if (!IsCheckpointRedoRecord(record))
            ereport(FATAL, "unexpected record type at redo point");
    } else {
        // Start after checkpoint
        replayTLI = CheckPointTLI;
        record = ReadRecord(xlogprefetcher, LOG, false, replayTLI);
    }

    if (record != NULL) {
        InRedo = true;
        RmgrStartup();

        ereport(LOG, "redo starts at %X/%X", LSN_FORMAT_ARGS(xlogreader->ReadRecPtr));

        // Main recovery loop
        do {
            // Handle interrupts and pause requests
            HandleStartupProcInterrupts();
            if (RecoveryIsPaused())
                recoveryPausesHere(false);

            // Check if we've reached recovery target
            if (recoveryStopsBefore(xlogreader)) {
                reachedRecoveryTarget = true;
                break;
            }

            // Apply delay if configured
            if (recoveryApplyDelay(xlogreader)) {
                if (RecoveryIsPaused())
                    recoveryPausesHere(false);
            }

            // Apply the WAL record
            ApplyWalRecord(xlogreader, record, &replayTLI);

            // Check recovery target after applying record
            if (recoveryStopsAfter(xlogreader)) {
                reachedRecoveryTarget = true;
                break;
            }

            // Read next record
            record = ReadRecord(xlogprefetcher, LOG, false, replayTLI);
        } while (record != NULL);

        // Handle recovery completion based on target action
        if (reachedRecoveryTarget) {
            switch (recoveryTargetAction) {
                case RECOVERY_TARGET_ACTION_SHUTDOWN:
                    proc_exit(3);
                case RECOVERY_TARGET_ACTION_PAUSE:
                    SetRecoveryPause(true);
                    recoveryPausesHere(true);
                case RECOVERY_TARGET_ACTION_PROMOTE:
                    break;
            }
        }

        RmgrCleanup();
        ereport(LOG, "redo done at %X/%X", LSN_FORMAT_ARGS(xlogreader->ReadRecPtr));

        InRedo = false;
    } else {
        ereport(LOG, "redo is not required");
    }

    // Verify recovery target was reached if required
    if (ArchiveRecoveryRequested && recoveryTarget != RECOVERY_TARGET_UNSET &&
        !reachedRecoveryTarget)
        ereport(FATAL, "recovery ended before configured recovery target was reached");
}
```

Key simplifications made:
- Abstracted complex shared memory initialization into InitializeRecoveryProgress()
- Simplified record type checking with IsCheckpointRedoRecord() helper
- Consolidated recovery pause checking into RecoveryIsPaused() helper
- Removed detailed debug logging and progress reporting code
- Focused on the main recovery flow: initialize, find start, replay loop, cleanup
- Preserved essential error handling and recovery target logic
- Removed low-level spinlock and timing details while maintaining core functionality