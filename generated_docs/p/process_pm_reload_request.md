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

## Simplified Source

```c
// Simplified version of process_pm_reload_request
static void process_pm_reload_request(void) {
    // Clear the reload request flag
    pending_pm_reload_request = false;

    // Log the reload request
    ereport(DEBUG2, (errmsg_internal("postmaster received reload request signal")));

    // Only proceed if not shutting down
    if (Shutdown <= SmartShutdown) {
        // Log that we're reloading configuration
        ereport(LOG, (errmsg("received SIGHUP, reloading configuration files")));

        // Step 1: Reload main configuration file (postgresql.conf)
        ProcessConfigFile(PGC_SIGHUP);

        // Step 2: Signal all child processes to reload their configs
        SignalChildren(SIGHUP);

        // Step 3: Signal specific auxiliary processes individually
        signal_auxiliary_processes();

        // Step 4: Reload authentication configuration files
        reload_auth_config();

        // Step 5: Handle SSL configuration reload (if enabled)
        reload_ssl_config();

        // Step 6: Update config for future children (EXEC_BACKEND only)
        update_exec_backend_config();
    }
}

// Helper functions (conceptual - actual implementation inlined above)
static void signal_auxiliary_processes(void) {
    // Send SIGHUP to all auxiliary processes if they're running
    // StartupPID, BgWriterPID, CheckpointerPID, WalWriterPID, etc.
}

static void reload_auth_config(void) {
    // Reload pg_hba.conf and pg_ident.conf
    // Log warnings if reload fails
}

static void reload_ssl_config(void) {
    // Reinitialize or destroy SSL configuration based on EnableSSL setting
}

static void update_exec_backend_config(void) {
    // Write updated configuration for future child processes (Windows)
}
```

Key simplifications made:
- Consolidated repetitive signal_child() calls into conceptual helper function
- Abstracted detailed error handling for auth and SSL config reloads
- Removed platform-specific conditional compilation details
- Focused on the main execution flow: clear flag → check shutdown → reload configs → signal processes
- Maintained the essential four-step process: main config, child signaling, auth config, SSL config