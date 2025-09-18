# pgwin32_dispatch_queued_signals

## Location
src/backend/port/win32/signal.c: 120 - 170

## Overview
pgwin32_dispatch_queued_signals processes all queued signals that are not currently blocked, executing their associated signal handlers.

## Definition
```c
void pgwin32_dispatch_queued_signals(void)
```

## Detailed Description
This function is the core signal dispatch mechanism for PostgreSQL on Windows. It processes all pending signals in the signal queue that are not currently blocked by the signal mask. The function operates within a critical section to ensure thread safety and handles signal delivery in a Unix-compatible manner.

The dispatch process works as follows:
1. **Queue Processing**: Iterates through all queued signals using the UNBLOCKED_SIGNAL_QUEUE() macro
2. **Signal Identification**: Examines each signal number to determine which ones need processing
3. **Handler Resolution**: Retrieves the appropriate signal handler, falling back to defaults if needed
4. **Signal Masking**: Temporarily blocks signals according to the handler's sa_mask and SA_NODEFER flag
5. **Handler Execution**: Calls the signal handler function with proper signal masking in place
6. **Queue Management**: Removes processed signals from the queue and resets the signal event

The function ensures that signal handlers execute with appropriate signal blocking behavior, mimicking Unix signal semantics on Windows.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - UNBLOCKED_SIGNAL_QUEUE (macro)
  - PG_SIGNAL_COUNT (signal count constant)
  - sigmask (signal mask utility)
  - [sigaction](../s/sigaction.md) (signal action structure)
  - SIG_DFL, SIG_ERR, SIG_IGN (signal constants)
  - sigset_t (signal set type)
  - SA_NODEFER (signal flag)
  - sigprocmask (signal mask function)
  - SIG_BLOCK, SIG_SETMASK (signal mask operations)
  - EnterCriticalSection/LeaveCriticalSection (Windows API)
  - ResetEvent (Windows API)
- Called from (representative examples):
  - [pg_usleep](pg_usleep.md)
  - [pqsigprocmask](pqsigprocmask.md)
  - [pgwin32_poll_signals](pgwin32_poll_signals.md)
  - [pgwin32_waitforsinglesocket](pgwin32_waitforsinglesocket.md)
  - [pgwin32_select](pgwin32_select.md)
  - [PGSemaphoreLock](../P/PGSemaphoreLock.md)
  - WaitEventSetWait

## Notes and Other Information
- This is a Windows-specific signal dispatch function located in src/backend/port/win32/signal.c
- The function operates within a critical section for thread safety
- It implements Unix-style signal masking behavior on Windows
- Signal handlers may modify the signal queue or mask, so the outer loop restarts after each handler execution
- The function resets the signal event after processing all queued signals
- Blocked signals are ignored and will be dispatched when unblocked via pqsigprocmask()
- The function handles signal re-entrancy by temporarily leaving the critical section during handler execution