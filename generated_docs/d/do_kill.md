# do_kill

## Location
[src/bin/pg_ctl/pg_ctl.c:1393-1405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1393-L1405)

## Overview
Sends a signal to a PostgreSQL process with error handling and proper exit status management.

## Definition


## Detailed Description
This function is a wrapper around the system's kill() function that sends a signal to a specified process ID. The function uses the global variable  to determine which signal to send. Key features include:

1. **Signal Delivery**: Uses the kill() system call to send the signal specified by the global  variable
2. **Error Handling**: Provides detailed error messages if the signal cannot be sent
3. **Process Termination**: Exits with code 1 if the signal delivery fails
4. **Internationalization**: Uses translatable error messages with the _() macro

The function is typically used by pg_ctl for sending various signals to PostgreSQL server processes, such as SIGTERM for shutdown, SIGHUP for configuration reload, etc.

## Parameters / Member Variables
- : The process ID of the target process to send the signal to

## Dependencies
- Functions called/Symbols referenced:
  - kill (system call)
  - [write_stderr](../w/write_stderr.md)
  - exit
  - sig (global variable)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pg_ctl/pg_ctl.c:2486)

## Notes and Other Information
- The function is static, meaning it's only used within pg_ctl.c
- Uses the global variable  to determine which signal to send
- Error messages include the signal number and target PID for debugging
- The %m format specifier in the error message automatically expands to the system error message (strerror equivalent)
- Never returns on failure - always exits with code 1 when signal delivery fails
- Part of pg_ctl's signal handling infrastructure for process management