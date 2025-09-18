# pqsignal

## Location
src/port/pqsignal.c: 135 - 175

## Overview
A reliable BSD-style signal handling function that provides consistent, portable signal semantics across different Unix systems and replaces the standard signal() function with more predictable behavior.

## Definition
```c
pqsigfunc pqsignal(int signo, pqsigfunc func)
```

## Detailed Description
The pqsignal function is PostgreSQL's implementation of a reliable signal handler installation mechanism, addressing the historical inconsistencies and race conditions present in traditional signal() implementations across different Unix systems. It wraps the POSIX sigaction() system call to provide explicit control over signal behavior while maintaining a simpler interface similar to the classic signal() function.

Key functionality includes:
- Uses sigaction() internally to ensure consistent "reliable" signal semantics (no automatic handler reset, proper signal blocking during handler execution)
- Sets SA_RESTART flag to automatically restart interrupted system calls
- For SIGCHLD, adds SA_NOCLDSTOP flag to prevent notification for stopped child processes
- Implements a wrapper mechanism: when a custom handler is provided, it stores the user handler in pqsignal_handlers[] and installs wrapper_handler as the actual system handler
- The wrapper_handler provides additional safety checks and errno preservation
- Handles both Unix systems (via sigaction) and Windows systems (via native signal forwarding)

This design ensures portable, predictable signal handling behavior across PostgreSQL's supported platforms while providing additional safety mechanisms for multi-process environments.

## Parameters / Member Variables
- `signo`: The signal number to handle (must be > 0 and < PG_NSIG)
- `func`: The signal handler function to install, or SIG_IGN/SIG_DFL for ignore/default behavior

## Dependencies
- Functions called/Symbols referenced:
  - [wrapper_handler](../w/wrapper_handler.md) (when installing custom handlers)
  - pqsignal_handlers (array to store original handlers)
  - [sigaction](../s/sigaction.md) (POSIX signal installation function)
  - sigemptyset (to initialize signal mask)
  - signal (on Windows frontend)
  - SIG_IGN, SIG_DFL, SIG_ERR (standard signal constants)
  - SA_RESTART, SA_NOCLDSTOP (sigaction flags)
- Called from (representative examples):
  - PostgreSQL initialization and signal setup code
  - Various PostgreSQL processes for installing custom signal handlers
  - Legacy libpq clients (via legacy-pqsignal.c wrapper)

## Notes and Other Information
- The function name may be compiled as pqsignal_fe when FRONTEND is defined to avoid conflicts with libpq's legacy version
- Return values may be bogus when called within signal handlers due to race conditions
- Two implementations exist: main version in src/port/pqsignal.c and legacy version in src/interfaces/libpq/legacy-pqsignal.c for backward compatibility with pre-9.3 libpq clients
- On Windows, provides limited emulation of reliable signals with different semantics than Unix
- Uses Assert() statements to validate signal number bounds
- Located in src/port/pqsignal.c:135-175