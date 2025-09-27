# enable_timeout_after

## Location
[src/backend/utils/misc/timeout.c:560-583](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L560-L583)

## Overview
Enables a specified timeout to fire after a given delay in milliseconds, providing a convenient interface for scheduling one-time timeouts.

## Definition
```c
void enable_timeout_after(TimeoutId id, int delay_ms)
```

## Detailed Description
The `enable_timeout_after` function provides a high-level interface for scheduling a timeout to fire after a specified delay from the current time. It is a convenience wrapper around the lower-level `enable_timeout` function, automatically calculating the absolute finish time based on the current timestamp and the requested delay.

The function performs these operations:
1. **Safety Preparation**: Disables alarm interrupts to ensure atomic timeout scheduling
2. **Time Calculation**: Gets current timestamp and calculates the absolute finish time by adding the delay
3. **Timeout Activation**: Calls the internal `enable_timeout` function with calculated timing (interval set to 0 for one-time execution)
4. **Alarm Scheduling**: Schedules the SIGALRM interrupt to fire at the appropriate time

This function is designed for one-time timeouts (not recurring) and is commonly used for statement timeouts, authentication timeouts, deadlock detection, and other time-limited operations in PostgreSQL.

## Parameters / Member Variables
- `id`: TimeoutId specifying which registered timeout to enable
- `delay_ms`: Delay in milliseconds after which the timeout should fire

## Dependencies
- Functions called/Symbols referenced:
  - [TimeoutId](../T/TimeoutId.md): Timeout identifier type
  - disable_alarm: Disables alarm system temporarily for safety
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md): Gets current system timestamp
  - TimestampTzPlusMilliseconds: Calculates future timestamp by adding milliseconds
  - [enable_timeout](enable_timeout.md): Internal function to activate timeout with absolute timing
  - [schedule_alarm](../s/schedule_alarm.md): Schedules the next SIGALRM interrupt
- Called from (representative examples):
  - [StartTransaction](../S/StartTransaction.md): Sets up transaction timeouts
  - [ProcSleep](../P/ProcSleep.md): Sets deadlock detection timeout during lock waits
  - [start_xact_command](../s/start_xact_command.md): Statement timeout activation
  - [ProcessInterrupts](../P/ProcessInterrupts.md): Query cancellation timeout handling
  - [assign_transaction_timeout](../a/assign_transaction_timeout.md): Transaction timeout configuration
  - [enable_statement_timeout](enable_statement_timeout.md): Statement execution timeout
  - [PerformAuthentication](../P/PerformAuthentication.md): Authentication timeout during connection

## Notes and Other Information
- Convenience function for relative timeout scheduling (as opposed to absolute time scheduling)
- Sets interval to 0, making this a one-time timeout rather than a recurring one
- Commonly used throughout PostgreSQL for various timeout scenarios including statement execution, authentication, and lock waits
- Automatically handles timestamp arithmetic to convert relative delays to absolute finish times
- Thread-safe within PostgreSQL single-threaded process model
- Requires the timeout to be previously registered with RegisterTimeout()
- Will overwrite any existing timeout for the same TimeoutId

## Simplified Source

```c
// Simplified version of enable_timeout_after
void enable_timeout_after(TimeoutId id, int delay_ms) {
    // Step 1: Disable interrupts for atomic operation
    disable_alarm();

    // Step 2: Calculate when timeout should fire
    TimestampTz current_time = GetCurrentTimestamp();
    TimestampTz finish_time = TimestampTzPlusMilliseconds(current_time, delay_ms);

    // Step 3: Activate the timeout (0 = one-time, not recurring)
    enable_timeout(id, current_time, finish_time, 0);

    // Step 4: Schedule the alarm interrupt
    schedule_alarm(current_time);
}
```

Key simplifications made:
- Combined variable declarations with assignments for clarity
- Added inline comments explaining each major step
- Used more descriptive variable names (current_time vs now, finish_time vs fin_time)
- Focused on the core logic flow without losing essential functionality