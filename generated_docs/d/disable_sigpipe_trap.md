# disable_sigpipe_trap

## Location
src/fe_utils/print.c: 3039 - 3061

## Overview
A utility function that disables SIGPIPE signal handling to prevent process termination when writing to broken pipes, particularly useful before writing to temporary query output files or pipes.

## Definition
```c
void disable_sigpipe_trap(void)
```

## Detailed Description
This function configures the process to ignore SIGPIPE signals by setting the signal handler to SIG_IGN (ignore). SIGPIPE signals are generated when a process attempts to write to a pipe that has been closed by the receiving end, which would normally terminate the process.

In PostgreSQL frontend utilities, this is critical when writing query output to pipes or temporary files that might be closed unexpectedly by external processes (like `less`, `more`, or other pagers). By ignoring SIGPIPE, the application can gracefully handle broken pipe situations and continue operation rather than being abruptly terminated.

The function is conditionally compiled and only has effect on Unix-like systems. On Windows, it's a no-op since Windows doesn't have SIGPIPE signals.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [pqsignal](../p/pqsignal.md) (PostgreSQL's signal handling wrapper)
  - SIGPIPE (signal constant)
  - SIG_IGN (signal handler constant for ignoring signals)
- Called from (representative examples):
  - [exec_command_write](../e/exec_command_write.md) (at src/bin/psql/command.c:2783)
  - [do_watch](do_watch.md) (at src/bin/psql/command.c:5409)
  - [SetupGOutput](../S/SetupGOutput.md) (at src/bin/psql/common.c:98)
  - [do_copy](do_copy.md) (at src/bin/psql/copy.c:310)
  - [PageOutput](../P/PageOutput.md) (at src/fe_utils/print.c:3123)

## Notes and Other Information
- This is a public function (not static), accessible from other modules
- Platform-specific: Only functional on Unix-like systems, no-op on Windows
- Uses pqsignal instead of standard signal() for PostgreSQL's signal handling consistency
- Should typically be paired with restore_sigpipe_trap() to restore original signal handling
- Critical for robust pipe handling in psql and other PostgreSQL frontend tools
- Part of PostgreSQL's frontend utilities for handling I/O operations safely
- The function is designed to be called before operations that might write to potentially unreliable pipes