# pgwin32_dispatch_queued_signals

## Location
[src/backend/port/win32/signal.c:120-170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/signal.c#L120-L170)

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
  - [WaitEventSetWait](../W/WaitEventSetWait.md)

## Notes and Other Information
- This is a Windows-specific signal dispatch function located in src/backend/port/win32/signal.c
- The function operates within a critical section for thread safety
- It implements Unix-style signal masking behavior on Windows
- Signal handlers may modify the signal queue or mask, so the outer loop restarts after each handler execution
- The function resets the signal event after processing all queued signals
- Blocked signals are ignored and will be dispatched when unblocked via pqsigprocmask()
- The function handles signal re-entrancy by temporarily leaving the critical section during handler execution

## Simplified Source

```c
// Simplified version of pgwin32_dispatch_queued_signals
void pgwin32_dispatch_queued_signals(void) {
    int exec_mask;

    // Enter critical section for thread safety
    EnterCriticalSection(&pg_signal_crit_sec);

    // Process all unblocked queued signals
    while ((exec_mask = UNBLOCKED_SIGNAL_QUEUE()) != 0) {
        // Check each signal in the queue
        for (int i = 1; i < PG_SIGNAL_COUNT; i++) {
            if (exec_mask & sigmask(i)) {
                // Get signal handler for this signal
                struct sigaction *act = &pg_signal_array[i];
                pqsigfunc sig = act->sa_handler;

                // Use default handler if needed
                if (sig == SIG_DFL)
                    sig = pg_signal_defaults[i];

                // Remove signal from queue
                pg_signal_queue &= ~sigmask(i);

                // Execute handler if valid
                if (sig != SIG_ERR && sig != SIG_IGN && sig != SIG_DFL) {
                    // Set up signal blocking during handler execution
                    sigset_t block_mask = act->sa_mask;
                    if ((act->sa_flags & SA_NODEFER) == 0)
                        block_mask |= sigmask(i);

                    // Temporarily leave critical section
                    LeaveCriticalSection(&pg_signal_crit_sec);

                    // Block signals, execute handler, restore signal mask
                    sigset_t save_mask;
                    sigprocmask(SIG_BLOCK, &block_mask, &save_mask);
                    sig(i);
                    sigprocmask(SIG_SETMASK, &save_mask, NULL);

                    // Re-enter critical section and restart loop
                    EnterCriticalSection(&pg_signal_crit_sec);
                    break;
                }
            }
        }
    }

    // Clean up and exit critical section
    ResetEvent(pgwin32_signal_event);
    LeaveCriticalSection(&pg_signal_crit_sec);
}
```

Key simplifications made:
- Added inline variable declarations for better readability
- Consolidated signal handler resolution logic
- Added descriptive comments for each major step
- Simplified the signal blocking explanation
- Focused on the main execution flow while preserving all essential logic
- Maintained the critical section handling and signal masking behavior