# do_stop

## Location
[src/bin/pg_ctl/pg_ctl.c:1015-1072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1015-L1072)

## Overview
Handles the stopping of a PostgreSQL server by sending a termination signal to the postmaster process and optionally waiting for the server to shut down completely.

## Definition


## Detailed Description
The  function is responsible for gracefully shutting down a PostgreSQL server instance. It first retrieves the process ID of the running postmaster from the PID file, validates that the server is running in the correct mode (not as a standalone backend), and then sends the appropriate termination signal to the process. 

The function handles several scenarios:
- If no PID file exists, it reports that the server is not running
- If a standalone backend is detected (negative PID), it prevents shutdown as single-user servers cannot be stopped via pg_ctl
- After sending the signal, it can either return immediately or wait for the server to completely shut down based on the  flag

The shutdown process respects the configured shutdown mode (smart, fast, or immediate) and provides appropriate feedback to the user throughout the process.

## Parameters / Member Variables
This function takes no parameters and operates on global variables:
- Uses global  variable to determine which signal to send
- Uses global  flag to determine whether to wait for shutdown
- Uses global  to provide context-specific error messages
- Uses global  and  for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [get_pgpid](../g/get_pgpid.md) - retrieves the postmaster process ID
  - kill - sends termination signal to the process
  - [wait_for_postmaster_stop](../w/wait_for_postmaster_stop.md) - waits for server shutdown completion
  - [write_stderr](../w/write_stderr.md) - outputs error messages
  - [print_msg](../p/print_msg.md) - outputs status messages
  - SMART_MODE - shutdown mode constant for hints

- Called from (representative examples):
  - [main](../m/main.md) - [main](../m/main.md) entry point of pg_ctl when stop action is requested

## Notes and Other Information
- The function exits with code 1 on any error condition
- Provides helpful hints when shutdown fails in smart mode, suggesting the use of fast mode
- Handles the special case of standalone backends which cannot be stopped via pg_ctl
- The actual signal sent depends on the shutdown mode configured globally (SIGTERM for smart/fast, SIGQUIT for immediate)
- Error messages are internationalized using the gettext system