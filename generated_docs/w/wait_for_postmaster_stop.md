# wait_for_postmaster_stop

## Location
[src/bin/pg_ctl/pg_ctl.c:709-745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L709-L745)

## Overview
Waits for the PostgreSQL postmaster process to stop completely, monitoring both the PID file removal and process liveness.

## Definition

```c
static bool
wait_for_postmaster_stop(void)
```
## Detailed Description
The  function implements a polling mechanism to monitor postmaster shutdown progress. It uses a dual-check approach to determine when the server has fully stopped:

1. **PID file monitoring**: Checks if the postmaster.pid file has been removed, which indicates clean shutdown
2. **Process liveness check**: Uses  to verify the process is no longer running

**Key behaviors:**
- Polls up to  times with sleep intervals between checks
- Returns  for clean shutdown (PID file removed)
- Returns  for timeout or unclean shutdown (process died but PID file remains)
- Includes race condition protection by double-checking the PID file after detecting process death
- Provides progress feedback by printing dots during the wait

The function helps distinguish between clean shutdowns (where PostgreSQL properly removes its PID file) and unclean shutdowns (where the process dies unexpectedly, leaving the PID file behind).

## Parameters / Member Variables
This function takes no parameters but relies on global variables:
- : Maximum time to wait for shutdown
- : Path to the PostgreSQL PID file (used by )

## Dependencies
- Functions called/Symbols referenced:
  -  - Reads the PID from postmaster.pid file
  -  - Tests if process is still alive (with signal 0)
  -  - Prints progress dots
  -  - Cross-platform sleep function
- Called from (representative examples):
  -  in pg_ctl.c
  -  in pg_ctl.c

## Notes and Other Information
- The function returns  only for clean shutdowns where the postmaster properly removes its PID file
- Race condition handling ensures reliable detection of shutdown completion
- The  technique is a standard Unix method to test process existence without sending an actual signal
- Progress indication helps users understand that shutdown monitoring is active
- Timeout behavior allows pg_ctl to avoid hanging indefinitely on problematic shutdowns
- Used by both stop and restart operations to ensure complete shutdown before proceeding