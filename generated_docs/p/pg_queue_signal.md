# pg_queue_signal

## Location
[src/backend/port/win32/signal.c:259-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/signal.c#L259-L273)

## Overview
Queues a signal for delivery to the main PostgreSQL thread on Windows by setting appropriate flags and triggering an event notification.

## Definition

```c
void
pg_queue_signal(int signum)
```
## Detailed Description
This function is a core component of PostgreSQL's Windows signal emulation system. It executes on the signal handler thread and safely queues signals for the main thread by setting bits in a signal queue bitmask and triggering an event. The function uses critical sections to ensure thread-safe access to the global signal queue variable.

The function validates the signal number against defined bounds and ignores invalid signals. Once a valid signal is queued, it sets the corresponding bit in the signal queue and signals the main thread via a Windows event object, allowing the main thread to process the queued signals at an appropriate time.

## Parameters / Member Variables
- : The signal number to queue. Must be between 1 and PG_SIGNAL_COUNT-1 (inclusive) to be considered valid.

## Dependencies
- Functions called/Symbols referenced:
  - Assert
  - EnterCriticalSection
  - LeaveCriticalSection
  - SetEvent (Windows API)
  - sigmask
  - PG_SIGNAL_COUNT
- Global variables accessed:
  - pgwin32_signal_event
  - pg_signal_crit_sec
  - pg_signal_queue
- Called from (representative examples):
  - [pg_signal_thread](pg_signal_thread.md) (in signal.c:327)
  - [pg_console_handler](pg_console_handler.md) (in signal.c:384)
  - [pg_timer_thread](pg_timer_thread.md) (in timer.c:68)
  - [pgwin32_deadchild_callback](pgwin32_deadchild_callback.md) (in postmaster.c:4668)

## Notes and Other Information
- Executes exclusively on the signal handler thread, requiring proper synchronization
- Uses critical sections to ensure thread-safe modification of pg_signal_queue
- Only the global variable pg_signal_queue can be safely accessed from this context
- Invalid signal numbers (≤0 or ≥PG_SIGNAL_COUNT) are silently ignored
- The sigmask() macro is used to convert signal numbers to bit positions
- SetEvent() notifies the main thread that signals are pending for processing
- Part of PostgreSQL's Windows signal emulation architecture

## Simplified Source

```c
// Simplified version of pg_queue_signal
void pg_queue_signal(int signum) {
    // Validate signal number is within acceptable range
    if (signum >= PG_SIGNAL_COUNT || signum <= 0) {
        return;  // Ignore invalid signal numbers
    }

    // Thread-safe signal queuing
    EnterCriticalSection(&pg_signal_crit_sec);
    pg_signal_queue |= sigmask(signum);  // Set the signal bit
    LeaveCriticalSection(&pg_signal_crit_sec);

    // Notify main thread that a signal is pending
    SetEvent(pgwin32_signal_event);
}
```

Key simplifications made:
- Removed Assert for clarity (assumes function preconditions are met)
- Added descriptive comments for each main operation
- Focused on the core signal queuing logic
- Maintained the essential thread synchronization and validation