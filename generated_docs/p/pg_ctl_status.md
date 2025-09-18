# pg_ctl_status

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 1413 - 1442

## Overview
A utility function that analyzes and reports the exit status of pg_ctl commands, providing detailed error messages and terminating the program on failure.

## Definition
static void pg_ctl_status(const char *pg_ctl_cmd, int rc)

## Detailed Description
This function serves as a centralized error handler for pg_ctl command executions within the pg_createsubscriber tool. It interprets the return code from pg_ctl operations and provides detailed, platform-specific error reporting.

The function handles three categories of failures:
1. **Normal Exit with Error**: When pg_ctl exits cleanly but with a non-zero exit code
2. **Signal Termination**: When pg_ctl is terminated by a signal (with platform-specific handling for Windows vs Unix-like systems)
3. **Unknown Status**: For any other unrecognized status codes

On any failure, the function logs detailed error information including the original command that failed, then terminates the entire program with exit(1), ensuring that pg_ctl failures are treated as fatal errors in the pg_createsubscriber workflow.

## Parameters / Member Variables
- : The complete pg_ctl command string that was executed (used for error reporting)
- : The return code from the pg_ctl execution (0 indicates success, non-zero indicates various failure modes)

## Dependencies
- Functions called/Symbols referenced:
  - WIFEXITED, WEXITSTATUS (POSIX macros for checking normal exit status)
  - WIFSIGNALED, WTERMSIG (POSIX macros for checking signal termination)
  - pg_log_error, pg_log_error_detail (PostgreSQL logging functions)
  - pg_strsignal (PostgreSQL utility for signal name resolution, Unix-like systems only)
  - exit (standard library function for program termination)

- Called from (representative examples):
  - [start_standby_server](../s/start_standby_server.md)
  - [stop_standby_server](../s/stop_standby_server.md)

## Notes and Other Information
- This is a static function specific to the pg_createsubscriber utility
- The function always terminates the program on any pg_ctl failure, making it unsuitable for recoverable error scenarios
- Platform-specific handling: Windows reports signals as hexadecimal exception codes with reference to ntstatus.h, while Unix-like systems provide signal names via pg_strsignal()
- The function assumes that a return code of 0 indicates success and does nothing in that case
- Part of the defensive programming approach in pg_createsubscriber to ensure database operations complete successfully
- Error messages include both the specific failure reason and the original command for debugging purposes