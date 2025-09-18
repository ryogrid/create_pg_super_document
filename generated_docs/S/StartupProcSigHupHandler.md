# StartupProcSigHupHandler

## Location
src/backend/postmaster/startup.c: 101 - 108

## Overview
A SIGHUP signal handler that triggers configuration file reloading by setting a flag and waking up the recovery process at the next convenient time.

## Definition
static void StartupProcSigHupHandler(SIGNAL_ARGS)

## Detailed Description
StartupProcSigHupHandler is a signal handler function designed to handle SIGHUP signals sent to the startup process during PostgreSQL recovery operations. When a SIGHUP signal is received, this handler sets the got_SIGHUP flag to true and calls WakeupRecovery() to ensure the recovery process processes the configuration reload request promptly. This mechanism allows PostgreSQL administrators to reload configuration changes during recovery without interrupting the recovery process flow.

The handler follows PostgreSQL's standard pattern for signal handling - it performs minimal work within the signal handler itself, only setting a flag and waking up the main process loop, which will then handle the actual configuration reloading at a safe point in execution.

## Parameters / Member Variables
- : Standard PostgreSQL signal handler arguments macro (typically expands to int signum for signal number)

## Dependencies
- Functions called/Symbols referenced:
  - [WakeupRecovery](../W/WakeupRecovery.md) (wakes up the recovery process)
  - SIGNAL_ARGS (signal handler arguments macro)
- Called from (representative examples):
  - [StartupProcessMain](StartupProcessMain.md) (registers this as SIGHUP handler)

## Notes and Other Information
- This handler is registered specifically for SIGHUP signals during startup process initialization
- Sets the global got_SIGHUP flag which is checked by the main recovery loop
- Configuration reloading occurs at the next convenient point in the recovery process
- Must be signal-safe and minimal to avoid race conditions and reentrancy issues
- Part of PostgreSQL's dynamic configuration management system during recovery operations
- The actual configuration file parsing and application happens asynchronously in the main process loop