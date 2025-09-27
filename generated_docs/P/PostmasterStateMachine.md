# PostmasterStateMachine

## Location
[src/backend/postmaster/postmaster.c:3128-3421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L3128-L3421)

## Overview
PostmasterStateMachine manages PostgreSQL's master state machine for coordinated shutdown, recovery, and restart operations based on the current process state and shutdown conditions.

## Definition
static void PostmasterStateMachine(void)

## Detailed Description
PostmasterStateMachine is the central coordination function that manages PostgreSQL's postmaster state transitions during shutdown, crash recovery, and restart scenarios. The function operates as a state machine with multiple states: PM_RUN/PM_HOT_STANDBY (normal operation), PM_STOP_BACKENDS (initiating shutdown), PM_WAIT_BACKENDS (waiting for backends to exit), PM_SHUTDOWN/PM_SHUTDOWN_2 (checkpoint and final cleanup), PM_WAIT_DEAD_END (waiting for dead-end processes), and PM_NO_CHILDREN (final state). During shutdown, it coordinates the orderly termination of different process types in sequence: first normal backends, then auxiliary processes like bgwriter and walwriter, followed by walsenders and archiver, and finally dead-end processes. For crash recovery, it handles reinitialization by cleaning up shared memory, removing temporary files, resetting background worker crash times, and restarting the startup process. The function also handles special cases like startup process failure and the restart_after_crash configuration option.

## Parameters / Member Variables
This function takes no parameters and operates on global postmaster state variables including pmState, FatalError, Shutdown, and various process PID tracking variables.

## Dependencies
- Functions called/Symbols referenced:
  - [CountChildren](../C/CountChildren.md)
  - [ForgetUnstartedBackgroundWorkers](../F/ForgetUnstartedBackgroundWorkers.md)
  - [SignalSomeChildren](../S/SignalSomeChildren.md)
  - [signal_child](../s/signal_child.md)
  - [StartChildProcess](../S/StartChildProcess.md)
  - [ConfigurePostmasterWaitSet](../C/ConfigurePostmasterWaitSet.md)
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - [ExitPostmaster](../E/ExitPostmaster.md)
  - [RemovePgTempFiles](../R/RemovePgTempFiles.md)
  - [ResetBackgroundWorkerCrashTimes](../R/ResetBackgroundWorkerCrashTimes.md)
  - [shmem_exit](../s/shmem_exit.md)
  - [LocalProcessControlFile](../L/LocalProcessControlFile.md)
  - [CreateSharedMemoryAndSemaphores](../C/CreateSharedMemoryAndSemaphores.md)
  - SignalChildren
- Called from (representative examples):
  - [process_pm_shutdown_request](../p/process_pm_shutdown_request.md)
  - [process_pm_child_exit](../p/process_pm_child_exit.md)
  - [process_pm_pmsignal](../p/process_pm_pmsignal.md)

## Notes and Other Information
- Implements PostgreSQL's coordinated shutdown sequence to ensure data consistency
- Handles both normal shutdown and crash recovery scenarios
- Uses state-based logic to manage complex process interdependencies during shutdown
- Critical for preventing conflicts between old and new postmaster instances during restart
- The syslogger process is treated specially and continues running throughout most shutdown phases
- Includes safety assertions to verify expected process states during transitions
- Supports immediate shutdown mode for emergency situations while still maintaining some coordination

## Simplified Source

```c
// Simplified version of PostmasterStateMachine
static void PostmasterStateMachine(void) {
    // Phase 1: Smart shutdown - wait for normal clients to disconnect
    if ((pmState == PM_RUN || pmState == PM_HOT_STANDBY) && !connsAllowed) {
        if (CountChildren(BACKEND_TYPE_NORMAL) == 0) {
            pmState = PM_STOP_BACKENDS;
        }
    }

    // Phase 2: Signal backends to shut down
    if (pmState == PM_STOP_BACKENDS) {
        // Forget pending background workers
        ForgetUnstartedBackgroundWorkers();

        // Signal all backend children except walsenders
        SignalSomeChildren(SIGTERM, BACKEND_TYPE_ALL - BACKEND_TYPE_WALSND);

        // Signal auxiliary processes
        signal_child(AutoVacPID, SIGTERM);
        signal_child(BgWriterPID, SIGTERM);
        signal_child(WalWriterPID, SIGTERM);
        signal_child(StartupPID, SIGTERM);
        signal_child(WalReceiverPID, SIGTERM);
        signal_child(WalSummarizerPID, SIGTERM);
        signal_child(SlotSyncWorkerPID, SIGTERM);

        pmState = PM_WAIT_BACKENDS;
    }

    // Phase 3: Wait for backends to exit
    if (pmState == PM_WAIT_BACKENDS) {
        bool all_backends_gone = (CountChildren(BACKEND_TYPE_ALL - BACKEND_TYPE_WALSND) == 0 &&
                                  StartupPID == 0 && WalReceiverPID == 0 &&
                                  WalSummarizerPID == 0 && BgWriterPID == 0 &&
                                  WalWriterPID == 0 && AutoVacPID == 0 &&
                                  SlotSyncWorkerPID == 0);

        if (all_backends_gone) {
            if (Shutdown >= ImmediateShutdown || FatalError) {
                // Skip checkpoint for immediate shutdown or crash
                pmState = PM_WAIT_DEAD_END;
            } else {
                // Normal shutdown: start checkpoint
                if (CheckpointerPID == 0) {
                    CheckpointerPID = StartChildProcess(B_CHECKPOINTER);
                }
                if (CheckpointerPID != 0) {
                    signal_child(CheckpointerPID, SIGUSR2);
                    pmState = PM_SHUTDOWN;
                } else {
                    // Checkpointer failed to start
                    FatalError = true;
                    pmState = PM_WAIT_DEAD_END;
                    SignalChildren(SIGQUIT);
                }
            }
        }
    }

    // Phase 4: Wait for walsenders and archiver
    if (pmState == PM_SHUTDOWN_2) {
        if (PgArchPID == 0 && CountChildren(BACKEND_TYPE_ALL) == 0) {
            pmState = PM_WAIT_DEAD_END;
        }
    }

    // Phase 5: Wait for dead-end processes
    if (pmState == PM_WAIT_DEAD_END) {
        ConfigurePostmasterWaitSet(false);

        if (dlist_is_empty(&BackendList) && PgArchPID == 0) {
            pmState = PM_NO_CHILDREN;
        }
    }

    // Phase 6: Final shutdown or restart decision
    if (pmState == PM_NO_CHILDREN) {
        if (Shutdown > NoShutdown) {
            // Shutdown requested - exit
            if (FatalError) {
                ereport(LOG, (errmsg("abnormal database system shutdown")));
                ExitPostmaster(1);
            } else {
                ExitPostmaster(0);
            }
        } else if (StartupStatus == STARTUP_CRASHED || !restart_after_crash) {
            // Don't restart after crash
            ExitPostmaster(1);
        }
    }

    // Phase 7: Crash recovery and restart
    if (FatalError && pmState == PM_NO_CHILDREN) {
        // Clean up after crash
        if (remove_temp_files_after_crash) {
            RemovePgTempFiles();
        }
        ResetBackgroundWorkerCrashTimes();

        // Reinitialize shared memory
        shmem_exit(1);
        LocalProcessControlFile(true);
        CreateSharedMemoryAndSemaphores();

        // Restart startup process
        StartupPID = StartChildProcess(B_STARTUP);
        StartupStatus = STARTUP_RUNNING;
        pmState = PM_STARTUP;
        AbortStartTime = 0;

        ConfigurePostmasterWaitSet(true);
    }
}
```

Key simplifications made:
- Consolidated similar process termination calls into logical groups
- Removed detailed error handling comments for clarity
- Simplified complex conditional logic into clearer boolean expressions
- Grouped related operations into distinct phases with descriptive comments
- Abstracted low-level PID checking into high-level state descriptions
- Focused on the main execution flow rather than edge cases
- Reduced repetitive assertions and detailed state validation