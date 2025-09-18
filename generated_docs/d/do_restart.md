# do_restart

## Location
src/bin/pg_ctl/pg_ctl.c: 1073 - 1136

## Overview
Handles the restarting of a PostgreSQL server by first stopping the running server (if any) and then starting a new instance.

## Definition
```c
static void
do_restart(void)
```

## Detailed Description
The `do_restart` function performs a complete restart operation of a PostgreSQL server. It follows a two-phase approach: first attempting to gracefully stop the existing server, then starting a new instance. The function is more robust than separate stop/start operations as it handles various edge cases:

- If no server is running (no PID file), it proceeds directly to starting a new server
- If a standalone backend is detected, it prevents the restart and instructs the user to terminate it manually
- If the server process appears to be dead (PID file exists but process is not alive), it proceeds with starting a new server
- For running servers, it always waits for complete shutdown before attempting to start the new instance

The function provides comprehensive error handling and user feedback throughout the restart process, ensuring that users understand what's happening at each step.

## Parameters / Member Variables
This function takes no parameters and operates on global variables:
- Uses global `sig` variable to determine which shutdown signal to send
- Uses global `shutdown_mode` for providing context-specific error messages
- Uses global `progname` and `pid_file` for error reporting and PID file location

## Dependencies
- Functions called/Symbols referenced:
  - get_pgpid - retrieves the current postmaster process ID
  - postmaster_is_alive - checks if a given PID corresponds to a running postmaster
  - kill - sends termination signal to the existing server
  - wait_for_postmaster_stop - waits for server shutdown completion
  - do_start - starts a new server instance
  - write_stderr - outputs error messages
  - print_msg - outputs status messages
  - SMART_MODE - shutdown mode constant for providing hints

- Called from (representative examples):
  - main - main entry point of pg_ctl when restart action is requested

## Notes and Other Information
- Unlike `do_stop`, this function always waits for shutdown completion before proceeding to start
- Provides specific error handling for standalone backends, which cannot be restarted via pg_ctl
- More forgiving than separate stop/start operations - if the server appears to be gone, it proceeds with startup anyway
- Includes helpful hints for users when shutdown fails in smart mode
- The restart operation maintains the same configuration and data directory as the previous instance
- Error messages are internationalized using the gettext system