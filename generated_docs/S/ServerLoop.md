# ServerLoop

## Location
[src/backend/postmaster/postmaster.c:1626-1836](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L1626-L1836)

## Overview
The main event loop of the PostgreSQL postmaster process that handles client connections, manages background processes, and performs periodic maintenance tasks.

## Definition

```c
static int
ServerLoop(void)
```
## Detailed Description
ServerLoop is the heart of the PostgreSQL postmaster, implementing the main event-driven loop that keeps the database server operational. This function runs continuously until server shutdown, orchestrating all major server operations:

**Core Event Handling:**
- Uses WaitEventSetWait() to efficiently monitor multiple events simultaneously
- Processes latch signals for internal communication (shutdown, reload, child exit, pmsignal)
- Accepts new client connections and forks backend processes via BackendStartup()
- Handles connection requests with high priority, ensuring responsive client service

**Process Management:**
- Monitors and restarts critical background processes (checkpointer, background writer, WAL writer)
- Manages autovacuum launcher based on system state and configuration
- Starts archiver processes when needed
- Coordinates slot synchronization and WAL receiver processes
- Handles background worker lifecycle through maybe_start_bgworkers()

**System Maintenance:**
- Performs periodic lock file validation (every minute) to detect external tampering
- Updates socket file timestamps (every 58 minutes) to prevent cleanup by system tasks
- Manages graceful and immediate shutdown sequences
- Enforces SIGKILL timeout for unresponsive child processes during shutdown

The loop uses DetermineSleepTime() to calculate optimal wait durations, balancing responsiveness with system efficiency.

## Parameters / Member Variables
- Local variables:
  - : Timestamp for periodic lock file validation
  - : Timestamp for socket file maintenance
  - : Array to store wait events
  - : Count of triggered events

## Dependencies
- Functions called/Symbols referenced:
  - [ConfigurePostmasterWaitSet](../C/ConfigurePostmasterWaitSet.md)
  - [WaitEventSetWait](../W/WaitEventSetWait.md)
  - [DetermineSleepTime](../D/DetermineSleepTime.md)
  - [ResetLatch](../R/ResetLatch.md)
  - [AcceptConnection](../A/AcceptConnection.md)
  - [BackendStartup](../B/BackendStartup.md)
  - [StartChildProcess](StartChildProcess.md)
  - [SysLogger_Start](SysLogger_Start.md)
  - [MaybeStartWalReceiver](../M/MaybeStartWalReceiver.md)
  - [MaybeStartWalSummarizer](../M/MaybeStartWalSummarizer.md)
  - [MaybeStartSlotSyncWorker](../M/MaybeStartSlotSyncWorker.md)
  - maybe_start_bgworkers
  - [TerminateChildren](../T/TerminateChildren.md)
  - [RecheckDataDirLockFile](../R/RecheckDataDirLockFile.md)
  - [TouchSocketFiles](../T/TouchSocketFiles.md)
  - [TouchSocketLockFiles](../T/TouchSocketLockFiles.md)
- Process request handlers:
  - [process_pm_shutdown_request](../p/process_pm_shutdown_request.md)
  - [process_pm_reload_request](../p/process_pm_reload_request.md)  
  - [process_pm_child_exit](../p/process_pm_child_exit.md)
  - [process_pm_pmsignal](../p/process_pm_pmsignal.md)
- Constants used:
  - WL_LATCH_SET, WL_SOCKET_ACCEPT
  - PM_RUN, PM_RECOVERY, PM_HOT_STANDBY, PM_STARTUP
  - B_CHECKPOINTER, B_BG_WRITER, B_WAL_WRITER, B_AUTOVAC_LAUNCHER, B_ARCHIVER
- Called from:
  - [PostmasterMain](../P/PostmasterMain.md)

## Notes and Other Information
- The function runs in an infinite loop until the postmaster shuts down
- High-priority requests (shutdown, reload) are processed unconditionally, even without latch events
- Socket acceptance creates ClientSocket structures that are cleaned up after backend startup
- Process restart logic varies by process type and server state (PM_RUN vs PM_RECOVERY etc.)
- Lock file validation prevents multiple postmasters from running on the same data directory
- Socket file touching prevents aggressive /tmp cleaners from removing active Unix sockets
- The loop includes PostgreSQL threading assertions when compiled with appropriate flags
- SIGKILL enforcement provides a last resort for unresponsive backends during shutdown

## Simplified Source

```c
// Simplified version of ServerLoop
static int ServerLoop(void) {
    time_t last_lockfile_recheck_time, last_touch_time;
    WaitEvent events[MAXLISTEN];
    int nevents;

    // Initialize wait set and timing
    ConfigurePostmasterWaitSet(true);
    last_lockfile_recheck_time = last_touch_time = time(NULL);

    // Main event loop
    for (;;) {
        time_t now;

        // Wait for events (connections, signals, timeouts)
        nevents = WaitEventSetWait(pm_wait_set, DetermineSleepTime(),
                                  events, lengthof(events), 0);

        // Process all triggered events
        for (int i = 0; i < nevents; i++) {
            // Reset latch if set
            if (events[i].events & WL_LATCH_SET)
                ResetLatch(MyLatch);

            // Handle high-priority requests immediately
            if (pending_pm_shutdown_request)
                process_pm_shutdown_request();
            if (pending_pm_reload_request)
                process_pm_reload_request();
            if (pending_pm_child_exit)
                process_pm_child_exit();
            if (pending_pm_pmsignal)
                process_pm_pmsignal();

            // Accept new client connections
            if (events[i].events & WL_SOCKET_ACCEPT) {
                ClientSocket client_socket;
                if (AcceptConnection(events[i].fd, &client_socket) == STATUS_OK)
                    BackendStartup(&client_socket);
                // Clean up socket
                if (client_socket.sock != PGINVALID_SOCKET)
                    closesocket(client_socket.sock);
            }
        }

        // Restart essential background processes if needed
        if (SysLoggerPID == 0 && Logging_collector)
            SysLoggerPID = SysLogger_Start();

        // Start core processes when in operational states
        if (pmState == PM_RUN || pmState == PM_RECOVERY ||
            pmState == PM_HOT_STANDBY || pmState == PM_STARTUP) {
            if (CheckpointerPID == 0)
                CheckpointerPID = StartChildProcess(B_CHECKPOINTER);
            if (BgWriterPID == 0)
                BgWriterPID = StartChildProcess(B_BG_WRITER);
        }

        // Start other specialized processes when appropriate
        if (WalWriterPID == 0 && pmState == PM_RUN)
            WalWriterPID = StartChildProcess(B_WAL_WRITER);

        if (AutoVacPID == 0 && should_start_autovacuum())
            AutoVacPID = StartChildProcess(B_AUTOVAC_LAUNCHER);

        if (PgArchPID == 0 && PgArchStartupAllowed())
            PgArchPID = StartChildProcess(B_ARCHIVER);

        // Handle other process management
        MaybeStartSlotSyncWorker();
        MaybeStartWalReceiver();
        MaybeStartWalSummarizer();
        maybe_start_bgworkers();

        // Periodic maintenance tasks
        now = time(NULL);

        // Force shutdown of stuck processes during shutdown
        if (shutdown_in_progress() && children_need_forced_termination(now)) {
            TerminateChildren(send_abort_for_kill ? SIGABRT : SIGKILL);
            AbortStartTime = 0;
        }

        // Validate lock file periodically (every minute)
        if (now - last_lockfile_recheck_time >= 60) {
            if (!RecheckDataDirLockFile()) {
                // Data directory compromised, emergency shutdown
                kill(MyProcPid, SIGQUIT);
            }
            last_lockfile_recheck_time = now;
        }

        // Touch socket files to prevent cleanup (every 58 minutes)
        if (now - last_touch_time >= 58 * 60) {
            TouchSocketFiles();
            TouchSocketLockFiles();
            last_touch_time = now;
        }
    }
}

// Helper function abstractions for clarity
static bool should_start_autovacuum(void) {
    return !IsBinaryUpgrade &&
           (AutoVacuumingActive() || start_autovac_launcher) &&
           pmState == PM_RUN;
}

static bool shutdown_in_progress(void) {
    return (Shutdown >= ImmediateShutdown || FatalError);
}

static bool children_need_forced_termination(time_t now) {
    return AbortStartTime != 0 &&
           (now - AbortStartTime) >= SIGKILL_CHILDREN_AFTER_SECS;
}
```

Key simplifications made:
- Removed detailed error handling and logging for clarity
- Abstracted complex conditional logic into helper functions
- Consolidated similar process startup patterns
- Simplified socket cleanup logic
- Focused on the main execution flow rather than edge cases
- Removed platform-specific code sections
- Used more descriptive variable names where helpful
- Added brief comments explaining each major section