# trap_sigint_during_startup

## Location
[src/bin/pg_ctl/pg_ctl.c:849-866](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L849-L866)

## Overview
A SIGINT signal handler that forwards interrupt signals to the PostgreSQL postmaster process during startup, enabling graceful shutdown when users press CTRL-C while waiting for the server to start.

## Definition
```c
static void trap_sigint_during_startup(SIGNAL_ARGS)
```

## Detailed Description
This function serves as a specialized signal handler for SIGINT signals received during PostgreSQL server startup. Its primary purpose is to provide a clean way to abort server startup when the user interrupts the process (typically with CTRL-C).

The function implements a two-step termination process:
1. First, it forwards the SIGINT signal to the postmaster process (if one exists) to request graceful shutdown
2. Then, it restores the default signal handler and re-raises the signal to terminate pg_ctl itself

This approach ensures that both the server startup process and pg_ctl terminate cleanly, preventing orphaned processes or incomplete startup states.

## Parameters / Member Variables
- `SIGNAL_ARGS`: Standard PostgreSQL signal handler argument macro (typically includes signal number and context)

The function operates on these global variables:
- `postmasterPID`: Process ID of the PostgreSQL server process
- `postgres_signal_arg`: The signal number that triggered the handler
- `progname`: Program name for error messages

## Dependencies
- Functions called/Symbols referenced:
  - `SIGNAL_ARGS` (signal handler argument macro)
  - `kill` (system call to send signals to processes)
  - [write_stderr](../w/write_stderr.md) (error output function)
  - `SIG_DFL` (default signal handler constant)
  - [pqsignal](../p/pqsignal.md) (PostgreSQL signal handling utility)
  - `raise` (standard C function to raise signals)

- Called from:
  - [do_start](../d/do_start.md) (installed as signal handler during server startup)

## Notes and Other Information
- This handler is specifically designed for use during server startup phase only
- The function checks if `postmasterPID` is valid (-1 indicates no postmaster process) before attempting to send signals
- Error handling includes logging if the signal forwarding to postmaster fails, but continues with pg_ctl termination regardless
- The two-step termination (forward signal, then default handling) ensures proper cleanup and prevents signal handling loops
- This mechanism prevents the common issue of orphaned server processes when startup is interrupted
- The use of `pqsignal` instead of standard `signal` provides PostgreSQL-specific signal handling behavior

## Simplified Source

```c
static void trap_sigint_during_startup(SIGNAL_ARGS) {
    // If we have a postmaster process, forward the signal to it
    if (postmasterPID != -1) {
        if (kill(postmasterPID, SIGINT) != 0)
            write_stderr(_("%s: could not send stop signal (PID: %d): %m\n"),
                         progname, (int) postmasterPID);
    }

    // Restore default signal handler and re-raise signal to terminate pg_ctl
    pqsignal(postgres_signal_arg, SIG_DFL);
    raise(postgres_signal_arg);
}
```