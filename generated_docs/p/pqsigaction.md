# pqsigaction

## Location
src/backend/port/win32/signal.c: 210 - 226

## Overview
pqsigaction is the Windows-specific implementation of the POSIX sigaction function for installing and examining signal handlers.

## Definition
```c
int pqsigaction(int signum, const struct sigaction *act, struct sigaction *oldact)
```

## Detailed Description
This function provides a Windows implementation of the POSIX sigaction system call, allowing PostgreSQL to install and manage signal handlers in a portable way. It operates on the global signal handler array (pg_signal_array) and provides standard POSIX semantics for signal handler installation.

The function performs the following operations:
1. **Parameter Validation**: Validates that the signal number is within the valid range (0 to PG_SIGNAL_COUNT)
2. **Old Handler Retrieval**: If oldact is provided, stores the current signal action configuration
3. **New Handler Installation**: If act is provided, installs the new signal action configuration

Key characteristics:
- Simple array-based storage of signal handlers
- Only operates on the main thread (no synchronization required)
- Provides POSIX-compatible signal handler management
- Supports retrieving current handlers without modification
- Validates signal numbers to prevent array bounds violations

## Parameters / Member Variables
- `signum`: The signal number to install or examine the handler for
- `act`: Pointer to the new signal action to install (can be NULL to only retrieve current handler)
- `oldact`: Pointer to store the previous signal action configuration (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - sigaction (struct type)
  - PG_SIGNAL_COUNT (maximum signal number constant)
  - EINVAL (errno constant)
  - pg_signal_array (global signal handler array)
- Called from (representative examples):
  - sigaction (via header mapping)

## Notes and Other Information
- This is a Windows-specific implementation located in src/backend/port/win32/signal.c
- The function is mapped to the standard sigaction name via preprocessor macros in header files
- Only intended for use on the main thread; no synchronization is performed
- Returns 0 on success, -1 on error (with errno set to EINVAL for invalid signal numbers)
- Signal handlers are stored in the global pg_signal_array indexed by signal number
- Provides POSIX-compatible signal action semantics on Windows platforms
- Does not perform any immediate signal processing; signal dispatch occurs separately via pgwin32_dispatch_queued_signals