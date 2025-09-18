# exec_prog

## Location
src/bin/pg_upgrade/exec.c: 85 - 91

## Overview
Executes external programs with stdout/stderr redirection and error handling, primarily used by pg_upgrade for running PostgreSQL utilities and commands during the upgrade process.

## Definition
```c
bool exec_prog(const char *log_filename, const char *opt_log_file,
               bool report_error, bool exit_on_error, const char *fmt, ...)
               pg_attribute_printf(5, 6);
```

## Detailed Description
The `exec_prog` function is a core utility in pg_upgrade that provides a standardized way to execute external programs with proper logging and error handling. It formats a command using printf-style formatting, logs the command being executed, redirects stdout and stderr to log files, and handles both successful and failed executions appropriately.

The function includes platform-specific handling for Windows to deal with file locking issues and thread synchronization. On Windows, it tracks the main thread ID to handle log file access properly when called from different threads.

Key behaviors:
- Formats command using variadic arguments with printf-style formatting
- Redirects command output to specified log files using shell redirection
- Logs the command being executed for debugging purposes
- Handles Windows-specific file locking and threading issues
- Provides configurable error reporting and program termination on failure
- Returns success/failure status for caller decision making

## Parameters / Member Variables
- `log_filename`: Name of the log file (relative to log directory) where command output will be redirected
- `opt_log_file`: Optional additional log file path for error messages (can be NULL)
- `report_error`: Whether to report errors to the user if command fails
- `exit_on_error`: Whether to terminate the program if command fails (when combined with report_error)
- `fmt`: Printf-style format string for the command to execute
- `...`: Variadic arguments for the format string

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (C standard library)
  - vsnprintf (C standard library) 
  - system (C standard library)
  - fopen, fclose, fprintf (C standard library)
  - [pg_fatal](../p/pg_fatal.md) (pg_upgrade utility)
  - [pg_log](../p/pg_log.md) (pg_upgrade logging)
  - [pg_usleep](../p/pg_usleep.md) (pg_upgrade utility)
  - report_status (pg_upgrade reporting)
  - GetCurrentThreadId (Windows API)
  
- Called from (representative examples):
  - [generate_old_dump](../g/generate_old_dump.md) (src/bin/pg_upgrade/dump.c:23)
  - [parallel_exec_prog](../p/parallel_exec_prog.md) (src/bin/pg_upgrade/parallel.c:81, 124)
  - [win32_exec_prog](../w/win32_exec_prog.md) (src/bin/pg_upgrade/parallel.c:157)
  - [main](../m/main.md) (src/bin/pg_upgrade/pg_upgrade.c:193, 217)
  - [prepare_new_cluster](../p/prepare_new_cluster.md) (src/bin/pg_upgrade/pg_upgrade.c:492, 505)
  - [prepare_new_globals](../p/prepare_new_globals.md) (src/bin/pg_upgrade/pg_upgrade.c:526)
  - [create_new_objects](../c/create_new_objects.md) (src/bin/pg_upgrade/pg_upgrade.c:569)
  - [copy_xact_xlog_xid](../c/copy_xact_xlog_xid.md) (src/bin/pg_upgrade/pg_upgrade.c:714, 722, 726, 731, 757, 785, 796)
  - [start_postmaster](../s/start_postmaster.md) (src/bin/pg_upgrade/server.c:262)
  - [stop_postmaster](../s/stop_postmaster.md) (src/bin/pg_upgrade/server.c:342)

## Notes and Other Information
- **Thread Safety**: On Windows, the function tracks the main thread ID and handles log file access differently for non-main threads to avoid file sharing violations
- **Platform Differences**: Windows implementation includes retry logic for log file opening and special handling for file locking issues
- **Command Length Limit**: Commands are limited to MAXCMDLEN (2 * MAXPGPATH) characters
- **Error Handling**: Supports both non-fatal error reporting and fatal program termination based on parameters
- **Logging**: All executed commands are logged with PG_VERBOSE level for debugging purposes
- **Output Redirection**: Uses shell redirection (`>> "logfile" 2>&1`) to capture both stdout and stderr
- **Usage Context**: Primarily used within pg_upgrade for executing PostgreSQL utilities like pg_dump, pg_restore, and pg_ctl during database upgrades