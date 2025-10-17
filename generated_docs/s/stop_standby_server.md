# stop_standby_server

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:1487-1510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L1487-L1510)

## Overview
A utility function that safely stops a PostgreSQL standby server using pg_ctl during the pg_createsubscriber process.

## Definition
static void stop_standby_server(const char *datadir)

## Detailed Description
This function provides a clean way to shut down a PostgreSQL standby server during the pg_createsubscriber workflow. It constructs and executes a pg_ctl stop command targeting the specified data directory.

The function performs the following operations:
1. **Command Construction**: Builds a pg_ctl stop command with the provided data directory path, using silent mode (-s) for cleaner output
2. **Shell Execution**: Executes the command using the system() function
3. **Error Handling**: Uses pg_ctl_status() to handle any shutdown failures (which will terminate the program on error)
4. **State Management**: Updates the global standby_running flag to reflect the server's stopped state
5. **Logging**: Provides debug information about the command being executed and confirmation when the server is stopped

The function uses psprintf() for safe string formatting and automatically handles proper quoting of the pg_ctl path and data directory.

## Parameters / Member Variables
- : Path to the PostgreSQL data directory of the server to be stopped

## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md) (PostgreSQL's safe string formatting function)
  - system (standard library function for shell command execution)
  - [pg_ctl_status](../p/pg_ctl_status.md) (error handling for pg_ctl operations)
  - pg_log_debug, pg_log_info (PostgreSQL logging functions)

- Called from (representative examples):
  - [cleanup_objects_atexit](../c/cleanup_objects_atexit.md)
  - [wait_for_end_recovery](../w/wait_for_end_recovery.md)
  - [main](../m/main.md) (multiple call sites in pg_createsubscriber)

## Notes and Other Information
- This is a static function specific to the pg_createsubscriber utility
- Uses silent mode (-s) for pg_ctl to reduce output noise
- Automatically handles path quoting for directories containing spaces
- Updates the global standby_running flag to maintain accurate server state tracking
- The function will terminate the program via pg_ctl_status() if the server fails to stop
- Used in both normal workflow and cleanup scenarios (including atexit cleanup)
- Memory for the command string is managed automatically by psprintf()
- Part of the server lifecycle management in the pg_createsubscriber tool

## Simplified Source

```c
static void stop_standby_server(const char *datadir)
{
    // Build pg_ctl stop command with data directory
    char *pg_ctl_cmd = psprintf("\"%s\" stop -D \"%s\" -s", pg_ctl_path, datadir);

    // Execute the stop command
    pg_log_debug("pg_ctl command is: %s", pg_ctl_cmd);
    int rc = system(pg_ctl_cmd);
    pg_ctl_status(pg_ctl_cmd, rc);  // Handles errors and exits if stop fails

    // Update server state and log success
    standby_running = false;
    pg_log_info("server was stopped");
}
```