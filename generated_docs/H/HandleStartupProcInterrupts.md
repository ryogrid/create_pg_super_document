# HandleStartupProcInterrupts

## Location
[src/backend/postmaster/startup.c:154-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/startup.c#L154-L202)

## Overview
A central interrupt handling function that processes various signals and requests sent to the startup process during PostgreSQL recovery operations.

## Definition
void HandleStartupProcInterrupts(void)

## Detailed Description
HandleStartupProcInterrupts serves as the primary interrupt processing function for the startup process during PostgreSQL recovery. It handles multiple types of interrupts and system events in a coordinated manner:

1. **SIGHUP Processing**: When got_SIGHUP is set, it calls StartupRereadConfig() to reload configuration files and potentially restart the WAL receiver if critical settings changed.

2. **Shutdown Requests**: Checks the shutdown_requested flag and immediately exits with status code 1 if shutdown was requested via SIGTERM.

3. **Postmaster Health Monitoring**: Implements emergency bailout logic if the postmaster process has died, using PostmasterIsAlive() checks. On systems with POSTMASTER_POLL_RATE_LIMIT, this check is performed at reduced frequency to minimize overhead.

4. **Barrier Event Processing**: Handles process signal barriers through ProcessProcSignalBarrier() when ProcSignalBarrierPending is set.

5. **Memory Context Logging**: Processes memory context logging requests via ProcessLogMemoryContextInterrupt() when LogMemoryContextPending is set.

The function is called frequently during recovery operations to ensure prompt handling of administrative requests and system events while maintaining recovery progress.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [StartupRereadConfig](../S/StartupRereadConfig.md) (handles configuration reloading)
  - [proc_exit](../p/proc_exit.md) (terminates process with exit code)
  - [PostmasterIsAlive](../P/PostmasterIsAlive.md) (checks if postmaster process is still alive)
  - [ProcessProcSignalBarrier](../P/ProcessProcSignalBarrier.md) (processes barrier events)
  - [ProcessLogMemoryContextInterrupt](../P/ProcessLogMemoryContextInterrupt.md) (handles memory context logging)
  - POSTMASTER_POLL_RATE_LIMIT (conditional compilation macro for rate limiting)
- Called from (representative examples):
  - [PerformWalRecovery](../P/PerformWalRecovery.md) (during WAL recovery operations)
  - [recoveryPausesHere](../r/recoveryPausesHere.md) (when recovery is paused)
  - [recoveryApplyDelay](../r/recoveryApplyDelay.md) (during recovery delay periods)
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md) (while waiting for WAL data)

## Notes and Other Information
- Central point for processing startup process interrupts during recovery
- Handles multiple types of system events and administrative requests
- Implements rate-limited postmaster health checking on systems that support it
- Must be called regularly during recovery operations to ensure responsiveness
- Part of PostgreSQL's recovery process management and coordination system
- Ensures prompt handling of configuration changes, shutdown requests, and system events
- Emergency bailout mechanism provides automatic cleanup when postmaster fails
- Static counter for postmaster polling helps reduce system call overhead on some platforms

## Simplified Source

```c
// Simplified version of HandleStartupProcInterrupts
void HandleStartupProcInterrupts(void) {
    // Process configuration reload signal
    if (got_SIGHUP) {
        got_SIGHUP = false;
        StartupRereadConfig();
    }

    // Handle shutdown request
    if (shutdown_requested)
        proc_exit(1);

    // Emergency bailout if postmaster has died
    if (IsUnderPostmaster && !PostmasterIsAlive())
        exit(1);

    // Process barrier events
    if (ProcSignalBarrierPending)
        ProcessProcSignalBarrier();

    // Handle memory context logging
    if (LogMemoryContextPending)
        ProcessLogMemoryContextInterrupt();
}
```

Key simplifications made:
- Removed rate limiting logic for clearer flow (POSTMASTER_POLL_RATE_LIMIT)
- Removed static counter and polling optimization for readability
- Consolidated postmaster health check into simple condition
- Maintained all core interrupt handling paths
- Preserved essential signal processing and emergency bailout logic