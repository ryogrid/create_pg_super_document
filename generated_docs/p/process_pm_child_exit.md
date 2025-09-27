# process_pm_child_exit

## Location
[src/backend/postmaster/postmaster.c:2354-2695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L2354-L2695)

## Overview
Handles cleanup and state transitions when child processes exit, managing different types of PostgreSQL processes and coordinating proper shutdown or restart procedures.

## Definition
```c
static void process_pm_child_exit(void)
```

## Detailed Description
This function is responsible for processing child process exits in the PostgreSQL postmaster. It uses `waitpid()` with WNOHANG to collect information about terminated child processes and handles different types of processes appropriately:

- **Startup Process**: Critical for database initialization and recovery
- **Background Writer**: Handles dirty buffer writes  
- **Checkpointer**: Manages checkpoint operations
- **WAL Writer**: Writes WAL buffers to disk
- **WAL Receiver**: Receives WAL from primary in standby mode
- **WAL Summarizer**: Creates WAL summaries
- **Autovacuum Launcher**: Coordinates automatic vacuuming
- **Archiver**: Archives completed WAL files
- **System Logger**: Handles logging infrastructure
- **Slot Sync Worker**: Synchronizes replication slots
- **Background Workers**: Custom background processes
- **Backend Processes**: Client connection handlers

The function determines whether to restart processes, crash the system, or transition to shutdown states based on the process type and exit status.

## Parameters / Member Variables
This function operates on global state variables including:
- `pending_pm_child_exit`: Flag indicating child exit pending
- Various process ID variables (StartupPID, BgWriterPID, etc.)
- `pmState`: Current postmaster state
- `Shutdown`: Current shutdown mode

## Dependencies
- Functions called/Symbols referenced:
  - [waitpid](../w/waitpid.md) - Collects child process exit information
  - [HandleChildCrash](../H/HandleChildCrash.md) - Handles abnormal process exits
  - [StartChildProcess](../S/StartChildProcess.md) - Restarts terminated processes
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md) - Advances postmaster state
  - [CleanupBackgroundWorker](../C/CleanupBackgroundWorker.md) - Handles background worker cleanup
  - [CleanupBackend](../C/CleanupBackend.md) - Handles backend process cleanup
  - [TerminateChildren](../T/TerminateChildren.md) - Sends termination signals
  - [ExitPostmaster](../E/ExitPostmaster.md) - Exits postmaster process
- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md) - Main postmaster event loop

## Notes and Other Information
- Uses a loop with `waitpid(-1, &exitstatus, WNOHANG)` to collect all pending child exit notifications
- Different process types have different restart policies and crash handling
- The startup process is treated specially as its failure can be catastrophic
- Normal exits (status 0) and FATAL exits (status 1) are generally handled gracefully
- The function coordinates with the postmaster state machine to manage system-wide state transitions
- Background workers and regular backends have their own specialized cleanup functions
- System logger is restarted immediately for safety when it exits
- Process restart decisions depend on current postmaster state and shutdown mode

## Simplified Source

```c
// Simplified version of process_pm_child_exit
static void process_pm_child_exit(void) {
    int pid;           // process id of dead child process
    int exitstatus;    // its exit status

    pending_pm_child_exit = false;

    // Collect all dead child processes
    while ((pid = waitpid(-1, &exitstatus, WNOHANG)) > 0) {

        // Handle startup process exit
        if (pid == StartupPID) {
            StartupPID = 0;

            // Normal shutdown or recovery target reached
            if (Shutdown > NoShutdown && (EXIT_STATUS_0(exitstatus) || EXIT_STATUS_1(exitstatus))) {
                StartupStatus = STARTUP_NOT_RUNNING;
                pmState = PM_WAIT_BACKENDS;
                continue;
            }

            // Recovery target reached - initiate shutdown
            if (EXIT_STATUS_3(exitstatus)) {
                StartupStatus = STARTUP_NOT_RUNNING;
                Shutdown = Max(Shutdown, SmartShutdown);
                TerminateChildren(SIGTERM);
                pmState = PM_WAIT_BACKENDS;
                continue;
            }

            // Catastrophic failure during startup
            if (pmState == PM_STARTUP && !EXIT_STATUS_0(exitstatus)) {
                LogChildExit(LOG, "startup process", pid, exitstatus);
                ExitPostmaster(1);
            }

            // Handle unexpected exit after startup
            if (!EXIT_STATUS_0(exitstatus)) {
                if (StartupStatus == STARTUP_SIGNALED) {
                    StartupStatus = STARTUP_NOT_RUNNING;
                    if (pmState == PM_STARTUP)
                        pmState = PM_WAIT_BACKENDS;
                } else {
                    StartupStatus = STARTUP_CRASHED;
                }
                HandleChildCrash(pid, exitstatus, "startup process");
                continue;
            }

            // Successful startup - transition to normal operations
            StartupStatus = STARTUP_NOT_RUNNING;
            pmState = PM_RUN;
            connsAllowed = true;

            // Start background processes
            if (CheckpointerPID == 0)
                CheckpointerPID = StartChildProcess(B_CHECKPOINTER);
            if (BgWriterPID == 0)
                BgWriterPID = StartChildProcess(B_BG_WRITER);
            if (WalWriterPID == 0)
                WalWriterPID = StartChildProcess(B_WAL_WRITER);

            // Start other auxiliary processes
            start_auxiliary_processes();
            maybe_start_bgworkers();

            continue;
        }

        // Handle background writer process
        if (pid == BgWriterPID) {
            BgWriterPID = 0;
            if (!EXIT_STATUS_0(exitstatus))
                HandleChildCrash(pid, exitstatus, "background writer process");
            continue;
        }

        // Handle checkpointer process
        if (pid == CheckpointerPID) {
            CheckpointerPID = 0;
            if (EXIT_STATUS_0(exitstatus) && pmState == PM_SHUTDOWN) {
                // Normal shutdown checkpoint completed
                signal_final_processes();
                pmState = PM_SHUTDOWN_2;
            } else {
                // Unexpected checkpointer exit
                HandleChildCrash(pid, exitstatus, "checkpointer process");
            }
            continue;
        }

        // Handle WAL writer process
        if (pid == WalWriterPID) {
            WalWriterPID = 0;
            if (!EXIT_STATUS_0(exitstatus))
                HandleChildCrash(pid, exitstatus, "WAL writer process");
            continue;
        }

        // Handle other auxiliary processes (WAL receiver, summarizer, etc.)
        if (handle_auxiliary_process_exit(pid, exitstatus))
            continue;

        // Handle background workers
        if (CleanupBackgroundWorker(pid, exitstatus)) {
            HaveCrashedWorker = true;
            continue;
        }

        // Handle regular backend processes
        CleanupBackend(pid, exitstatus);
    }

    // Check for state changes and take appropriate actions
    PostmasterStateMachine();
}

// Helper function to start auxiliary processes
static void start_auxiliary_processes(void) {
    MaybeStartWalSummarizer();

    if (!IsBinaryUpgrade && AutoVacuumingActive() && AutoVacPID == 0)
        AutoVacPID = StartChildProcess(B_AUTOVAC_LAUNCHER);
    if (PgArchStartupAllowed() && PgArchPID == 0)
        PgArchPID = StartChildProcess(B_ARCHIVER);

    MaybeStartSlotSyncWorker();
}

// Helper function to signal final processes during shutdown
static void signal_final_processes(void) {
    if (PgArchPID != 0)
        signal_child(PgArchPID, SIGUSR2);
    SignalChildren(SIGUSR2);
}

// Helper function to handle auxiliary process exits
static bool handle_auxiliary_process_exit(int pid, int exitstatus) {
    // Handle WAL receiver, WAL summarizer, autovacuum launcher,
    // archiver, system logger, and slot sync worker exits
    // Returns true if process was handled, false otherwise

    if (pid == WalReceiverPID) {
        WalReceiverPID = 0;
        if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
            HandleChildCrash(pid, exitstatus, "WAL receiver process");
        return true;
    }

    // Similar handling for other auxiliary processes...

    return false;
}
```

Key simplifications made:
- Consolidated repetitive process handling into helper functions
- Abstracted auxiliary process exit handling into a separate function
- Removed detailed error checking and platform-specific code
- Simplified conditional logic while preserving essential flow
- Combined similar process handling patterns
- Focused on the main execution path and core state transitions
- Removed verbose logging and status reporting details