# handle_sig_alarm

## Location
[src/bin/pgbench/pgbench.c:7747-7752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L7747-L7752)

## Overview
Signal handler for SIGALRM that processes active timeout events and reschedules future alarm interrupts in PostgreSQL's timeout management system.

## Definition


## Detailed Description
This function serves as the core signal handler for PostgreSQL's timeout management system. When a SIGALRM signal is received, it processes all pending timeouts that have reached their scheduled firing time by calling their registered handler functions. The function maintains a priority queue of active timeouts sorted by firing time and processes them in chronological order. For recurring timeouts, it automatically reschedules the next occurrence, carefully handling drift by basing the next firing time on the intended rather than actual firing time (unless significantly delayed). The function operates within PostgreSQL's interrupt handling framework, using HOLD_INTERRUPTS/RESUME_INTERRUPTS to prevent recursive signal processing and ensuring thread safety.

## Parameters / Member Variables
- Uses SIGNAL_ARGS macro which expands to standard signal handler parameters (signal number)

## Dependencies
- Functions called/Symbols referenced:
  - HOLD_INTERRUPTS/RESUME_INTERRUPTS (interrupt control)
  - [SetLatch](../S/SetLatch.md) (process latch signaling) 
  - disable_alarm/schedule_alarm (alarm management)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (current time retrieval)
  - remove_timeout_index (timeout queue management)
  - TimestampTzPlusMilliseconds (time arithmetic)
  - enable_timeout (timeout rescheduling)
  - timeout_params (timeout configuration structure)
- Called from (representative examples):
  - InitializeTimeouts (at line 493 for signal handler registration)

## Notes and Other Information
- Always sets MyLatch to wake up any processes waiting on the process latch
- Resets signal_pending flag regardless of alarm_enabled state
- Only processes timeouts when alarm_enabled is true
- Handles recurring timeouts by automatically rescheduling based on interval_in_ms
- Guards against timeout drift by using intended firing times for rescheduling
- Updates current time after each timeout handler to account for handler execution time
- Uses timeout indicator flags to mark timeouts as fired for external checking
- Part of PostgreSQL's comprehensive timeout management infrastructure used throughout the system