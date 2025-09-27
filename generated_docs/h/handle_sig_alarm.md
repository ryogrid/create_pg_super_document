# handle_sig_alarm

## Location
[src/bin/pgbench/pgbench.c:7747-7752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L7747-L7752)

## Overview
Signal handler for SIGALRM that processes active timeout events and reschedules future alarm interrupts in PostgreSQL's timeout management system.

## Definition

```c
static void
handle_sig_alarm(SIGNAL_ARGS)
```
## Detailed Description
This function serves as the core signal handler for PostgreSQL's timeout management system. When a SIGALRM signal is received, it processes all pending timeouts that have reached their scheduled firing time by calling their registered handler functions. The function maintains a priority queue of active timeouts sorted by firing time and processes them in chronological order. For recurring timeouts, it automatically reschedules the next occurrence, carefully handling drift by basing the next firing time on the intended rather than actual firing time (unless significantly delayed). The function operates within PostgreSQL's interrupt handling framework, using HOLD_INTERRUPTS/RESUME_INTERRUPTS to prevent recursive signal processing and ensuring thread safety.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - HOLD_INTERRUPTS/RESUME_INTERRUPTS (interrupt control)
  - [SetLatch](../S/SetLatch.md) (process latch signaling) 
  - disable_alarm/schedule_alarm (alarm management)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (current time retrieval)
  - [remove_timeout_index](../r/remove_timeout_index.md) (timeout queue management)
  - TimestampTzPlusMilliseconds (time arithmetic)
  - [enable_timeout](../e/enable_timeout.md) (timeout rescheduling)
  - [timeout_params](../t/timeout_params.md) (timeout configuration structure)
- Called from (representative examples):
  - [InitializeTimeouts](../I/InitializeTimeouts.md) (at line 493 for signal handler registration)

## Notes and Other Information
- Always sets MyLatch to wake up any processes waiting on the process latch
- Resets signal_pending flag regardless of alarm_enabled state
- Only processes timeouts when alarm_enabled is true
- Handles recurring timeouts by automatically rescheduling based on interval_in_ms
- Guards against timeout drift by using intended firing times for rescheduling
- Updates current time after each timeout handler to account for handler execution time
- Uses timeout indicator flags to mark timeouts as fired for external checking
- Part of PostgreSQL's comprehensive timeout management infrastructure used throughout the system

## Simplified Source

```c
// Simplified version of handle_sig_alarm
static void handle_sig_alarm(SIGNAL_ARGS) {
    // Prevent interrupt processing during signal handling
    HOLD_INTERRUPTS();

    // Wake up any waiting processes
    SetLatch(MyLatch);

    // Clear signal pending flag
    signal_pending = false;

    // Process timeouts only if alarms are enabled
    if (alarm_enabled) {
        // Disable alarms to prevent re-entry
        disable_alarm();

        if (num_active_timeouts > 0) {
            TimestampTz now = GetCurrentTimestamp();

            // Process all expired timeouts
            while (num_active_timeouts > 0 &&
                   now >= active_timeouts[0]->fin_time) {

                timeout_params *expired_timeout = active_timeouts[0];

                // Remove from active list and mark as fired
                remove_timeout_index(0);
                expired_timeout->indicator = true;

                // Execute the timeout handler
                expired_timeout->timeout_handler();

                // Reschedule if it's a recurring timeout
                if (expired_timeout->interval_in_ms > 0) {
                    TimestampTz next_time = expired_timeout->fin_time +
                                          expired_timeout->interval_in_ms;

                    // Adjust for drift - if we're too late, base on current time
                    if (next_time < now) {
                        next_time = now + expired_timeout->interval_in_ms;
                    }

                    enable_timeout(expired_timeout->index, now, next_time,
                                 expired_timeout->interval_in_ms);
                }

                // Update current time after handler execution
                now = GetCurrentTimestamp();
            }

            // Schedule next alarm if timeouts remain
            schedule_alarm(now);
        }
    }

    // Re-enable interrupt processing
    RESUME_INTERRUPTS();
}
```

Key simplifications made:
- Removed detailed comments and consolidated similar logic
- Simplified time calculation expressions for better readability
- Focused on the main execution flow: signal handling → timeout processing → rescheduling
- Abstracted complex timestamp arithmetic into clearer variable assignments
- Maintained all essential algorithm steps and error prevention measures