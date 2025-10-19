# trapsig

## Location
[src/bin/initdb/initdb.c:2080-2090](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2080-L2090)

## Overview
A signal handler function that safely manages interruption signals during initdb execution by setting a flag instead of directly calling exit().

## Definition
```c
static void trapsig(SIGNAL_ARGS)
```

## Detailed Description
The `trapsig` function serves as a signal handler for various interruption signals during the initdb process. It implements a safe approach to signal handling by:

1. Setting a boolean flag (`caught_signal`) instead of directly calling exit()
2. Re-registering itself as the signal handler to handle systems that reset handlers
3. Avoiding forbidden operations in signal handlers (especially on Windows)

The function is designed with special consideration for Windows platform limitations, where signal handlers have strict restrictions on what operations can be performed (no IO, memory allocation, or system calls). The implementation uses a flag-based approach that can be checked by the main program flow to handle cleanup gracefully.

## Parameters / Member Variables
- `SIGNAL_ARGS`: Standard signal handler arguments macro (typically includes signal number)

## Dependencies
- Functions called/Symbols referenced:
  - [pqsignal](../p/pqsignal.md) (PostgreSQL's signal registration function)
  - postgres_signal_arg (signal argument variable)
  - caught_signal (global boolean flag set when signal is caught)
  - SIGNAL_ARGS (macro defining signal handler parameters)

- Called from:
  - [setup_signals](../s/setup_signals.md) (registers trapsig for multiple signal types)
  - Self-reference (re-registers itself as handler)

## Notes and Other Information
- Designed to comply with Windows signal handler restrictions that forbid IO, memory allocation, and system calls
- Uses flag-based approach instead of direct exit() to allow proper cleanup
- Handles platform differences, especially Windows behavior with SIGINT that creates new threads
- Re-registers itself to handle systems that reset signal handlers after invocation
- The caught_signal flag should be checked periodically by the main program to detect interruptions
- Special consideration given to Windows' multithreading behavior with CTRL+C interrupts

## Simplified Source

```c
static void trapsig(SIGNAL_ARGS) {
    // Re-register handler for systems that reset it (like Windows)
    pqsignal(postgres_signal_arg, trapsig);

    // Set flag instead of calling exit() directly for safe signal handling
    caught_signal = true;
}
```