# set_cancel_handler

## Location
src/bin/pg_dump/parallel.c: 708 - 729

## Overview
Initializes the console interrupt handler for Windows platforms, ensuring that interrupt signals (Ctrl+C, Ctrl+Break) are properly handled during pg_dump operations.

## Definition
```c
static void set_cancel_handler(void)
```

## Detailed Description
set_cancel_handler is a Windows-specific initialization function that sets up the infrastructure needed for graceful handling of console interrupts. The function implements a one-time initialization pattern to ensure the handler is only set up once per process:

1. **Handler check**: Verifies that the handler hasn't already been installed using the signal_info.handler_set flag
2. **Flag setting**: Marks the handler as installed to prevent duplicate initialization
3. **Critical section initialization**: Initializes the Windows critical section used for thread synchronization during shutdown
4. **Console handler registration**: Registers the consoleHandler function with the Windows console subsystem

This function is called early in the pg_dump process to ensure that interrupt handling is available throughout the operation. The critical section initialized here is later used by consoleHandler to prevent race conditions when multiple threads access shared data during shutdown.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - InitializeCriticalSection (initializes Windows critical section object)
  - SetConsoleCtrlHandler (registers console control handler with Windows)
  - [consoleHandler](../c/consoleHandler.md) (the actual interrupt handler function)
- Global variables accessed:
  - signal_info.handler_set (flag to track initialization state)
  - signal_info_lock (critical section object for synchronization)
- Called from (representative examples):
  - [write_stderr](../w/write_stderr.md) (during error handling setup)
  - [set_archive_cancel_info](set_archive_cancel_info.md) (during archive operation setup)

## Notes and Other Information
- This is a Windows-only function (part of the WIN32 conditional compilation block)
- Function implements one-time initialization pattern to prevent duplicate handler registration
- The critical section initialized here is essential for thread-safe operation during shutdown
- Registers consoleHandler as the callback function for console control events
- Function is static and only used within the parallel.c module
- Must be called before any parallel operations begin to ensure proper interrupt handling
- The TRUE parameter to SetConsoleCtrlHandler indicates the handler should be added (not removed)
- Located in src/bin/pg_dump/parallel.c:708-729