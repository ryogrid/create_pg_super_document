# wait_pid

## Location
[src/test/regress/regress.c:692-711](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L692-L711)

## Overview
A PostgreSQL regression test function that waits for a specified process to terminate by continuously checking its liveness, restricted to superusers only for security purposes.

## Definition

```c
Datum
wait_pid(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL C function designed for use in regression testing environments to synchronize test execution by waiting for external processes to complete. It takes a process ID (PID) as an argument and continuously monitors the process using the  system call with signal 0 (which checks process existence without sending any signal). The function polls the process every 50 milliseconds until it no longer exists, then returns. The function enforces strict security by requiring superuser privileges and includes interrupt handling to allow cancellation during the wait period.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: Process ID to wait for (int32 type)

## Dependencies
- Functions called/Symbols referenced:
  - [superuser](../s/superuser.md): Checks if current user has superuser privileges
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

## Simplified Source

```c
Datum wait_pid(PG_FUNCTION_ARGS) {
    int pid = PG_GETARG_INT32(0);

    // Security check: only superusers can monitor processes
    if (!superuser())
        elog(ERROR, "must be superuser to check PID liveness");

    // Poll until process no longer exists
    while (kill(pid, 0) == 0) {
        CHECK_FOR_INTERRUPTS();  // Allow query cancellation
        pg_usleep(50000);        // Sleep 50ms between checks
    }

    // Verify process actually terminated (not other error)
    if (errno != ESRCH)
        elog(ERROR, "could not check PID %d liveness: %m", pid);

    PG_RETURN_VOID();
}
```