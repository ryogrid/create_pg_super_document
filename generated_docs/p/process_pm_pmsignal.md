# process_pm_pmsignal

## Location
[src/backend/postmaster/postmaster.c:3704-3861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L3704-L3861)

## Overview
process_pm_pmsignal handles pmsignal conditions representing requests from backends and checks for promote and logrotate requests from pg_ctl in the PostgreSQL postmaster process.

## Definition
static void process_pm_pmsignal(void)

## Detailed Description
process_pm_pmsignal is a comprehensive signal processing function in the postmaster that handles various inter-process communication signals from backend processes. It serves as the central dispatcher for postmaster state transitions and process management operations. The function processes multiple types of signals including recovery state changes, background worker management, autovacuum operations, WAL receiver management, log rotation requests, and promotion signals.

The function implements careful state checking to ensure signals are only processed in appropriate postmaster states, preventing race conditions that could occur when processes start up quickly or exit unexpectedly. It coordinates the startup sequence from recovery through hot standby mode, manages background worker lifecycle, and handles administrative operations like log rotation and database promotion.

## Parameters / Member Variables
- void: Takes no parameters, operates on global postmaster state variables

## Dependencies
- Functions called/Symbols referenced:
  - [CheckPostmasterSignal](../C/CheckPostmasterSignal.md) (multiple signal types)
  - ereport (for logging)
  - XLogArchivingAlways (archiving check)
  - [StartChildProcess](../S/StartChildProcess.md) (process spawning)
  - [AddToDataDirLockFile](../A/AddToDataDirLockFile.md) (status reporting)
  - [BackgroundWorkerStateChange](../B/BackgroundWorkerStateChange.md) (worker management)
  - maybe_start_bgworkers (worker startup)
  - [CheckLogrotateSignal](../C/CheckLogrotateSignal.md) (log rotation detection)
  - [signal_child](../s/signal_child.md) (process signaling)
  - [RemoveLogrotateSignalFiles](../R/RemoveLogrotateSignalFiles.md) (cleanup)
  - [StartAutovacuumWorker](../S/StartAutovacuumWorker.md) (autovacuum management)
  - [MaybeStartWalReceiver](../M/MaybeStartWalReceiver.md) (WAL receiver management)
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md) (state transitions)
  - [CheckPromoteSignal](../C/CheckPromoteSignal.md) (promotion detection)
- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md) (main postmaster event loop)

## Notes and Other Information
- Sets pending_pm_pmsignal to false at the beginning to clear the signal condition
- Includes systemd integration with sd_notify calls for service readiness notification
- Handles multiple recovery states: RECOVERY_STARTED, BEGIN_HOT_STANDBY with careful state validation
- Implements defensive programming against race conditions in process startup/shutdown sequences
- Background worker changes are accepted only when not in stopping state
- Autovacuum launcher can be started even when autovacuuming is disabled as defense against transaction ID wraparound
- The function ordering is important - [PostmasterStateMachine](../P/PostmasterStateMachine.md) is called before CheckPromoteSignal to ensure proper state evaluation

## Simplified Source

```c
// Simplified version of process_pm_pmsignal
static void process_pm_pmsignal(void) {
    // Clear the pending signal flag
    pending_pm_pmsignal = false;

    ereport(DEBUG2, (errmsg_internal("postmaster received pmsignal signal")));

    // Handle recovery startup signal
    if (CheckPostmasterSignal(PMSIGNAL_RECOVERY_STARTED) &&
        pmState == PM_STARTUP && Shutdown == NoShutdown) {

        // Reset error state and start archiver if needed
        FatalError = false;
        AbortStartTime = 0;

        if (XLogArchivingAlways()) {
            PgArchPID = StartChildProcess(B_ARCHIVER);
        }

        // Update status if not entering hot standby
        if (!EnableHotStandby) {
            AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STANDBY);
            // Notify systemd if available
        }

        pmState = PM_RECOVERY;
    }

    // Handle hot standby ready signal
    if (CheckPostmasterSignal(PMSIGNAL_BEGIN_HOT_STANDBY) &&
        pmState == PM_RECOVERY && Shutdown == NoShutdown) {

        ereport(LOG, (errmsg("database system is ready to accept read-only connections")));

        // Update status and allow connections
        AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_READY);
        pmState = PM_HOT_STANDBY;
        connsAllowed = true;
        StartWorkerNeeded = true;
    }

    // Handle background worker changes
    if (CheckPostmasterSignal(PMSIGNAL_BACKGROUND_WORKER_CHANGE)) {
        BackgroundWorkerStateChange(pmState < PM_STOP_BACKENDS);
        StartWorkerNeeded = true;
    }

    // Start workers if needed
    if (StartWorkerNeeded || HaveCrashedWorker) {
        maybe_start_bgworkers();
    }

    // Handle log rotation requests
    if (SysLoggerPID != 0) {
        if (CheckLogrotateSignal() || CheckPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE)) {
            signal_child(SysLoggerPID, SIGUSR1);
            if (CheckLogrotateSignal()) {
                RemoveLogrotateSignalFiles();
            }
        }
    }

    // Handle autovacuum launcher start request
    if (CheckPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER) &&
        Shutdown <= SmartShutdown && pmState < PM_STOP_BACKENDS) {
        start_autovac_launcher = true;
    }

    // Handle autovacuum worker start request
    if (CheckPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER) &&
        Shutdown <= SmartShutdown && pmState < PM_STOP_BACKENDS) {
        StartAutovacuumWorker();
    }

    // Handle WAL receiver start request
    if (CheckPostmasterSignal(PMSIGNAL_START_WALRECEIVER)) {
        WalReceiverRequested = true;
        MaybeStartWalReceiver();
    }

    // Advance state machine if requested
    if (CheckPostmasterSignal(PMSIGNAL_ADVANCE_STATE_MACHINE)) {
        PostmasterStateMachine();
    }

    // Handle promotion signal
    if (StartupPID != 0 &&
        (pmState == PM_STARTUP || pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY) &&
        CheckPromoteSignal()) {
        // Tell startup process to finish recovery and promote
        signal_child(StartupPID, SIGUSR2);
    }
}
```

Key simplifications made:
- Removed detailed comments explaining race conditions and kept essential logic comments
- Consolidated similar log rotation signal handling into one conditional block
- Abstracted systemd notification details with simple comment
- Simplified conditional expressions while preserving logic
- Focused on the main execution flow and signal processing sequence
- Maintained all critical state checks and signal handling logic
- Preserved the important ordering of operations (state machine before promotion check)