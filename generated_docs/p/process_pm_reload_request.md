# process_pm_reload_request

## Location
[src/backend/postmaster/postmaster.c:2096-2168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L2096-L2168)

## Overview
Processes configuration reload requests by re-reading config files and signaling all child processes to reload their configurations.

## Definition
```c
static void process_pm_reload_request(void)
```

## Detailed Description
process_pm_reload_request performs the actual work of reloading PostgreSQL configurations when triggered by a SIGHUP signal. This function is called from the postmaster's main event loop after the handle_pm_reload_request_signal() handler sets the pending_pm_reload_request flag.

The function performs a comprehensive reload process:

1. **Flag Management**: Clears the pending_pm_reload_request flag and logs the reload request
2. **Shutdown Check**: Only processes the reload if not in shutdown mode (beyond SmartShutdown)
3. **Core Configuration**: Reloads postgresql.conf using ProcessConfigFile(PGC_SIGHUP)
4. **Child Process Notification**: Sends SIGHUP to all active child processes including:
   - All backend processes via SignalChildren()
   - Specific auxiliary processes (StartupPID, BgWriterPID, CheckpointerPID, etc.)
5. **Authentication Configuration**: Reloads pg_hba.conf and pg_ident.conf files
6. **SSL Configuration**: Conditionally reloads SSL settings if enabled
7. **EXEC_BACKEND Support**: Updates configuration files for future child processes

This comprehensive approach ensures that configuration changes are propagated system-wide without requiring a server restart.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ereport
  - ProcessConfigFile
  - SignalChildren
  - [signal_child](../s/signal_child.md)
  - [load_hba](../l/load_hba.md)
  - [load_ident](../l/load_ident.md)
  - [secure_initialize](../s/secure_initialize.md) (SSL)
  - [secure_destroy](../s/secure_destroy.md) (SSL)
  - [write_nondefault_variables](../w/write_nondefault_variables.md) (EXEC_BACKEND)
- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md)

## Notes and Other Information
- Static function - only accessible within postmaster.c
- Called from the main event loop, not from signal handler context
- Checks shutdown state before processing to avoid issues during shutdown
- Handles platform-specific features (SSL, EXEC_BACKEND) conditionally
- Provides detailed logging for failed reloads
- Critical for live configuration updates in production environments
- Ensures all child processes receive reload notifications
- Part of PostgreSQL's hot configuration reload capability
- Essential for operational flexibility - allows configuration changes without downtime