# handle_pm_reload_request_signal

## Location
src/backend/postmaster/postmaster.c: 2086 - 2095

## Overview
Signal handler that processes SIGHUP signals from pg_ctl to request a reload of PostgreSQL configuration files.

## Definition
```c
static void handle_pm_reload_request_signal(SIGNAL_ARGS)
```

## Detailed Description
handle_pm_reload_request_signal is a signal handler function that responds to SIGHUP signals sent to the postmaster process. This signal is primarily used by pg_ctl to request that the postmaster reload its configuration files (postgresql.conf, pg_hba.conf, etc.) without requiring a full server restart.

The handler follows PostgreSQL's standard pattern for signal handling:
1. Sets a global flag (pending_pm_reload_request) to indicate a reload is pending
2. Wakes up the postmaster's main event loop using SetLatch() 

This design ensures that the actual configuration reload processing happens in the main event loop context rather than within the signal handler, which is safer and allows for proper error handling and logging. The signal handler itself only performs async-signal-safe operations.

## Parameters / Member Variables
- Uses SIGNAL_ARGS macro which expands to standard signal handler parameters (typically int sig)

## Dependencies
- Functions called/Symbols referenced:
  - [SetLatch](../S/SetLatch.md)
  - SIGNAL_ARGS (macro)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md) (as signal handler registration)

## Notes and Other Information
- Static function - only accessible within postmaster.c
- Registered as SIGHUP handler during PostmasterMain initialization
- Critical for allowing live configuration reloads via 'pg_ctl reload'
- The actual reload processing is deferred to process_pm_reload_request() in the main loop
- Uses only async-signal-safe operations within the handler
- Essential for PostgreSQL's operational flexibility - allows configuration changes without downtime
- Part of PostgreSQL's broader signal-based inter-process communication system
- Commonly triggered by database administrators using pg_ctl reload command