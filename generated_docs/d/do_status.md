# do_status

## Location
[src/bin/pg_ctl/pg_ctl.c:1336-1392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1336-L1392)

## Overview
Implements the status command functionality for pg_ctl, checking and reporting the current state of a PostgreSQL server instance.

## Definition

```c
static void
do_status(void)
```
## Detailed Description
This function checks whether a PostgreSQL server is currently running and provides detailed status information. The function performs the following operations:

1. **PID File Check**: Uses  to read the PID from the postmaster.pid file
2. **Process Type Detection**: Distinguishes between standalone backend processes (negative PID) and regular postmaster processes (positive PID)
3. **Liveness Verification**: Uses  to confirm the process is still running
4. **Status Reporting**: Provides detailed output including:
   - Process type (single-user server vs. postmaster)
   - Process ID
   - Server options (for postmaster processes)
5. **Exit Code Management**: Returns appropriate exit codes following Linux Standard Base specifications

For running postmaster processes, the function also reads and displays the server's command-line options from the postopts file, giving administrators insight into how the server was started.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [get_pgpid](../g/get_pgpid.md)
  - [postmaster_is_alive](../p/postmaster_is_alive.md)
  - [readfile](../r/readfile.md)
  - [free_readfile](../f/free_readfile.md)
  - printf/_()
  - puts
  - exit
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pg_ctl/pg_ctl.c:2465)

## Notes and Other Information
- The function is static, meaning it's only used within pg_ctl.c
- Handles both standalone backend processes (single-user mode) and regular postmaster processes
- Negative PIDs in the PID file indicate standalone backend processes
- Reads server options from the postopts file to provide comprehensive status information
- Exits with code 3 when no server is running, following Linux Standard Base Core Specification 3.1
- Uses internationalized strings with the _() macro for translatable output messages
- The function never returns normally when no server is running (calls exit(3))