# StartupProcShutdownHandler

## Location
[src/backend/postmaster/startup.c:109-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/startup.c#L109-L124)

## Overview
A SIGTERM signal handler that initiates shutdown of the startup process by either immediately exiting during restore commands or setting a shutdown flag for graceful termination.

## Definition
static void StartupProcShutdownHandler(SIGNAL_ARGS)

## Detailed Description
StartupProcShutdownHandler is a signal handler function that manages the shutdown procedure for the startup process when a SIGTERM signal is received. The function implements conditional behavior based on the current state: if the process is currently executing a restore command (in_restore_command is true), it immediately exits with status code 1 to avoid potential hanging or corruption issues. Otherwise, it sets the shutdown_requested flag to true and calls WakeupRecovery() to ensure the main recovery loop processes the shutdown request gracefully.

This dual-mode approach ensures that the startup process can be terminated quickly when necessary (during restore commands) while still allowing for clean shutdown procedures during normal recovery operations, preserving database consistency and proper resource cleanup.

## Parameters / Member Variables
- : Standard PostgreSQL signal handler arguments macro (typically expands to int signum for signal number)

## Dependencies
- Functions called/Symbols referenced:
  - [proc_exit](../p/proc_exit.md) (immediately terminates process with exit code)
  - [WakeupRecovery](../W/WakeupRecovery.md) (wakes up the recovery process)
  - SIGNAL_ARGS (signal handler arguments macro)
- Called from (representative examples):
  - [StartupProcessMain](StartupProcessMain.md) (registers this as SIGTERM handler)

## Notes and Other Information
- This handler is registered specifically for SIGTERM signals during startup process initialization
- Implements conditional shutdown logic based on in_restore_command global flag
- Immediate exit during restore commands prevents potential deadlocks or hanging scenarios
- Graceful shutdown via shutdown_requested flag during normal recovery allows proper cleanup
- Part of PostgreSQL's process management and shutdown coordination system
- The WakeupRecovery() call ensures the main loop processes the shutdown request promptly
- Must be signal-safe and minimal to avoid race conditions during shutdown procedures