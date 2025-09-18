# do_logrotate

## Location
[src/bin/pg_ctl/pg_ctl.c:1255-1311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1255-L1311)

## Overview
Triggers log file rotation for a running PostgreSQL server by creating a logrotate signal file and sending a rotation signal to the postmaster process.

## Definition
```c
static void
do_logrotate(void)
```

## Detailed Description
The `do_logrotate` function initiates log file rotation for a running PostgreSQL server. Log rotation is essential for managing disk space usage and organizing log files in production environments. When logging_collector is enabled and log files are being written, this function provides a way to force the server to close the current log file and start writing to a new one.

The log rotation process follows these steps:
1. Validates that the server is running and not in standalone mode
2. Creates a "logrotate" signal file in the data directory
3. Sends a SIGUSR1 signal to the postmaster process
4. The postmaster recognizes the logrotate file and initiates the rotation process
5. Provides confirmation that the rotation signal was sent

This mechanism is particularly useful for integration with external log management tools and scheduled maintenance tasks that need to ensure log files are rotated at specific intervals.

## Parameters / Member Variables
This function takes no parameters and operates on global variables:
- Uses global `pg_data` for the data directory path where the logrotate signal file is created
- Uses global `progname` for error reporting
- Uses global `pid_file` for locating the server process
- Uses global `logrotate_file` to store the constructed path to the logrotate signal file

## Dependencies
- Functions called/Symbols referenced:
  - [get_pgpid](../g/get_pgpid.md) - retrieves the postmaster process ID from the PID file
  - fopen/fclose - creates and closes the logrotate signal file
  - kill - sends SIGUSR1 signal to trigger log rotation
  - unlink - removes the logrotate signal file if signal sending fails
  - [write_stderr](../w/write_stderr.md) - outputs error messages
  - [print_msg](../p/print_msg.md) - outputs success confirmation message
  - SIGUSR1 - signal used to trigger log rotation processing

- Called from (representative examples):
  - [main](../m/main.md) - [main](../m/main.md) entry point of pg_ctl when logrotate action is requested

## Notes and Other Information
- Only works when the logging_collector is enabled in PostgreSQL configuration
- Cannot rotate logs for standalone backends as they don't support the logging collector
- The logrotate file serves as a signal mechanism that the postmaster checks during SIGUSR1 handling
- Uses SIGUSR1 signal, which is also used for promotion - the presence of the logrotate file distinguishes the intent
- The actual log rotation timing depends on the server's internal processing of the signal
- Useful for integration with external log rotation systems (like logrotate utility on Unix systems)
- Does not wait for the rotation to complete - returns immediately after sending the signal
- The server will only rotate logs if logging_collector is enabled and log_filename contains time-based patterns
- Cleans up the logrotate signal file if the signal cannot be sent to prevent stale files
- Error messages are internationalized using the gettext system