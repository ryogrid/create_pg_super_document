# SignalHandlerForCrashExit

## Location
[src/backend/postmaster/interrupt.c:73-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/interrupt.c#L73-L104)

## Overview
SignalHandlerForCrashExit is a signal handler designed for immediate process termination in crash scenarios, typically used for handling SIGQUIT signals when shared memory corruption is suspected.

## Definition
```c
void SignalHandlerForCrashExit(SIGNAL_ARGS)
```

## Detailed Description
This function implements a "crash exit" signal handler that performs an immediate, unclean process termination. Unlike normal exit procedures, this handler deliberately bypasses all cleanup routines, atexit callbacks, and transaction cleanup mechanisms. It is specifically designed for scenarios where shared memory corruption is suspected or when a process needs to terminate immediately without attempting any cleanup that could potentially cause further damage or deadlocks.

The handler uses _exit(2) instead of the standard exit(0) to signal to the postmaster that this was an abnormal termination requiring a system reset cycle. This behavior is crucial for maintaining database consistency and triggering appropriate recovery mechanisms when processes exit unexpectedly.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - SIGNAL_ARGS (signal handler parameter macro)
  - _exit() system call (with exit code 2)
- Called from (representative examples):
  - [InitPostmasterChild](../I/InitPostmasterChild.md) (src/backend/utils/init/miscinit.c:159)

## Notes and Other Information
- This signal handler is declared in src/include/postmaster/interrupt.h
- Typically used for handling SIGQUIT signals in PostgreSQL backend processes
- The handler intentionally does NOT call proc_exit() or any atexit() callbacks to avoid potential issues with corrupted shared memory
- Uses exit code 2 to signal abnormal termination to the postmaster, triggering system reset cycles
- The "dead man switch" mechanism in pmsignal.c works in conjunction with this handler to ensure the postmaster detects crashed processes
- This is a safety mechanism that prioritizes system stability over graceful cleanup when corruption is suspected
- The immediate termination without cleanup is by design - any attempt to clean up potentially corrupted state could cause further problems