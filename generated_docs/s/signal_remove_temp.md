# signal_remove_temp

## Location
[src/test/regress/pg_regress.c:479-499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L479-L499)

## Overview
A signal handler function that performs cleanup of temporary socket directories when pg_regress receives termination signals, then re-raises the original signal.

## Definition

```c
static void
signal_remove_temp(SIGNAL_ARGS)
```
## Detailed Description
This function serves as a signal handler for various termination signals during pg_regress execution. When a signal is received, it first calls remove_temp() to clean up the temporary socket directory and associated files, then restores the default signal handler and re-raises the same signal to maintain proper signal handling semantics. This ensures that temporary directories are cleaned up even when the program is terminated unexpectedly by signals.

## Parameters / Member Variables
- `SIGNAL_ARGS`: Standard PostgreSQL signal handler argument macro, typically expands to include the signal number via postgres_signal_arg
## Dependencies
- Functions called/Symbols referenced:
  - [remove_temp](../r/remove_temp.md)
  - [pqsignal](../p/pqsignal.md)
  - raise (system call)
  - SIG_DFL (constant)
  - postgres_signal_arg (global variable)
  - SIGNAL_ARGS (macro)
- Called from (representative examples):
  - [make_temp_sockdir](../m/make_temp_sockdir.md) (installed as signal handler for SIGINT, SIGTERM, SIGQUIT, SIGHUP)

## Notes and Other Information
- Function is marked static (internal to pg_regress.c)
- Implements the common pattern of cleanup-then-reraise for signal handlers
- Uses pqsignal to restore default signal handling before re-raising
- Installed as handler for multiple signals: SIGINT, SIGTERM, SIGQUIT, SIGHUP
- Essential for proper cleanup during abnormal program termination
- Works in conjunction with remove_temp() to ensure no temporary files are left behind
- Located in src/test/regress/pg_regress.c:479-499

## Simplified Source

```c
static void signal_remove_temp(SIGNAL_ARGS) {
    // Clean up temporary socket directory
    remove_temp();

    // Restore default signal handler and re-raise signal
    pqsignal(postgres_signal_arg, SIG_DFL);
    raise(postgres_signal_arg);
}
```