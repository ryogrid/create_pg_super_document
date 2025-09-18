# do_promote

## Location
src/bin/pg_ctl/pg_ctl.c: 1174 - 1254

## Overview
Promotes a PostgreSQL standby server to become a primary server by creating a promote signal file and sending a promotion signal to the postmaster process.

## Definition
```c
static void
do_promote(void)
```

## Detailed Description
The `do_promote` function handles the promotion of a PostgreSQL standby server (replica) to a primary (master) server. This is a critical operation in PostgreSQL streaming replication and high availability scenarios, typically used during failover situations when the primary server becomes unavailable.

The promotion process involves several steps:
1. Validates that the server is running and in the correct state for promotion
2. Checks that the server is actually in standby mode (archive recovery) 
3. Creates a "promote" signal file in the data directory
4. Sends a SIGUSR1 signal to the postmaster to trigger the promotion process
5. Optionally waits for the promotion to complete

The function includes comprehensive error handling and state validation to ensure that promotion only occurs when appropriate and safe.

## Parameters / Member Variables
This function takes no parameters and operates on global variables:
- Uses global `do_wait` flag to determine whether to wait for promotion completion
- Uses global `pg_data` for the data directory path where the promote file is created
- Uses global `progname` for error reporting
- Uses global `pid_file` for locating the server process

## Dependencies
- Functions called/Symbols referenced:
  - [get_pgpid](../g/get_pgpid.md) - retrieves the postmaster process ID from the PID file
  - [get_control_dbstate](../g/get_control_dbstate.md) - checks the current database state from control file
  - fopen/fclose - creates and closes the promote signal file
  - kill - sends SIGUSR1 signal to trigger promotion
  - [wait_for_postmaster_promote](../w/wait_for_postmaster_promote.md) - waits for promotion completion (if do_wait is true)
  - unlink - removes the promote file if signal sending fails
  - [write_stderr](../w/write_stderr.md) - outputs error messages
  - [print_msg](../p/print_msg.md) - outputs status messages
  - DB_IN_ARCHIVE_RECOVERY - database state constant for standby mode
  - SIGUSR1 - signal used to trigger promotion

- Called from (representative examples):
  - [main](../m/main.md) - [main](../m/main.md) entry point of pg_ctl when promote action is requested

## Notes and Other Information
- Only works on servers that are in standby/archive recovery mode - will fail on primary servers
- Cannot promote standalone backends as they don't support replication
- The promote file serves as both a signal mechanism and a safety check for the postmaster
- Uses SIGUSR1 signal specifically, which is the standard PostgreSQL promotion signal
- The promotion process is irreversible - once promoted, the server cannot automatically revert to standby
- If waiting is enabled, provides feedback on promotion progress and completion
- Cleans up the promote file if the signal cannot be sent to prevent confusion
- Critical for high availability and disaster recovery scenarios in PostgreSQL clusters
- Error messages are internationalized using the gettext system