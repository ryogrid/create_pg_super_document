# wait_pid

## Location
src/test/regress/regress.c: 692 - 711

## Overview
A PostgreSQL regression test function that waits for a specified process to terminate by continuously checking its liveness, restricted to superusers only for security purposes.

## Definition


## Detailed Description
The  function is a PostgreSQL C function designed for use in regression testing environments to synchronize test execution by waiting for external processes to complete. It takes a process ID (PID) as an argument and continuously monitors the process using the  system call with signal 0 (which checks process existence without sending any signal). The function polls the process every 50 milliseconds until it no longer exists, then returns. The function enforces strict security by requiring superuser privileges and includes interrupt handling to allow cancellation during the wait period.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: Process ID to wait for (int32 type)

## Dependencies
- Functions called/Symbols referenced:
  - superuser: Checks if current user has superuser privileges
  - kill: System call used to check process existence (with signal 0)
  - CHECK_FOR_INTERRUPTS: Macro to handle PostgreSQL interrupts during polling loop
  - [pg_usleep](../p/pg_usleep.md): PostgreSQL's microsecond sleep function (sleeps for 50ms)
  - PG_RETURN_VOID: Returns void from PostgreSQL function
- Called from (representative examples):
  - [regress_setenv](../r/regress_setenv.md): Referenced in the same regression test file

## Notes and Other Information
- This function is specifically designed for regression testing purposes and should not be used in production environments
- Requires superuser privileges to prevent unauthorized process monitoring
- Uses a polling mechanism with 50-millisecond intervals to check process liveness
- The kill(pid, 0) technique is a standard Unix method to check if a process exists without affecting it
- Includes proper interrupt handling via CHECK_FOR_INTERRUPTS() to allow query cancellation
- Returns only when the target process has terminated (errno == ESRCH) or when an unexpected error occurs
- Located in src/test/regress/regress.c as part of PostgreSQL's test infrastructure
- Any error other than "No such process" (ESRCH) results in an ERROR being raised