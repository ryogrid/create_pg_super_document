# start_standby_server

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 1443 - 1486

## Overview
A function that constructs and executes a pg_ctl command to start a PostgreSQL standby server with specific configuration options for the pg_createsubscriber tool.

## Definition
static void start_standby_server(const struct CreateSubscriberOptions *opt, bool restricted_access, bool restrict_logical_worker)

## Detailed Description
This function is responsible for starting a PostgreSQL standby server during the pg_createsubscriber process. It builds a comprehensive pg_ctl start command with various configuration options tailored for creating a subscriber from a standby.

Key features include:
1. **Base Command Construction**: Creates a pg_ctl start command with the subscriber data directory
2. **Sync Replication Slots Disabled**: Always starts with sync_replication_slots=off to prevent conflicts during setup
3. **Restricted Access Mode**: When enabled, configures the server for local-only access with Unix domain sockets (non-Windows platforms) and custom port
4. **Logical Worker Control**: Can disable logical replication workers during startup when needed
5. **Custom Configuration**: Supports custom config file specification
6. **Error Handling**: Uses pg_ctl_status() to handle any startup failures

The function sets the global standby_running flag upon successful startup and provides comprehensive logging throughout the process.

## Parameters / Member Variables
- : Pointer to CreateSubscriberOptions structure containing configuration like port, socket directory, and config file path
- : If true, configures the server for local-only access with restricted permissions
- : If true, disables logical replication workers by setting max_logical_replication_workers=0

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer, destroyPQExpBuffer, appendPQExpBuffer, appendPQExpBufferStr, appendPQExpBufferChar (PQExpBuffer utilities)
  - [appendShellString](../a/appendShellString.md) (shell-safe string escaping)
  - system (execute shell command)
  - [pg_ctl_status](../p/pg_ctl_status.md) (error handling for pg_ctl)
  - pg_log_debug, pg_log_info (logging functions)
  - [CreateSubscriberOptions](../C/CreateSubscriberOptions.md) (configuration structure type)

- Called from (representative examples):
  - [main](../m/main.md) (multiple call sites in pg_createsubscriber)

## Notes and Other Information
- This is a static function specific to the pg_createsubscriber utility
- Always disables sync_replication_slots to prevent startup conflicts
- Platform-specific behavior: restricted access mode uses Unix domain sockets on non-Windows platforms only
- Sets the global standby_running flag to track server state
- The function will terminate the program via pg_ctl_status() if the server fails to start
- Uses shell string escaping for safe command execution
- Supports custom socket directories and config files through the options structure
- Part of the pg_createsubscriber workflow for converting a standby server to a subscriber