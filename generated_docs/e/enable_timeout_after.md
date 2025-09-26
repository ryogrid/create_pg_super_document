# enable_timeout_after

## Location
src/backend/utils/misc/timeout.c: 560 - 583

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
  - TimeoutId: Timeout identifier type
  - disable_alarm: Disables alarm system temporarily for safety
  - GetCurrentTimestamp: Gets current system timestamp
  - TimestampTzPlusMilliseconds: Calculates future timestamp by adding milliseconds
  - enable_timeout: Internal function to activate timeout with absolute timing
  - schedule_alarm: Schedules the next SIGALRM interrupt
- Called from (representative examples):
  - StartTransaction: Sets up transaction timeouts
  - ProcSleep: Sets deadlock detection timeout during lock waits
  - start_xact_command: Statement timeout activation
  - ProcessInterrupts: Query cancellation timeout handling
  - assign_transaction_timeout: Transaction timeout configuration
  - enable_statement_timeout: Statement execution timeout
  - PerformAuthentication: Authentication timeout during connection

## Notes and Other Information
- Convenience function for relative timeout scheduling (as opposed to absolute time scheduling)
- Sets interval to 0, making this a one-time timeout rather than a recurring one
- Commonly used throughout PostgreSQL for various timeout scenarios including statement execution, authentication, and lock waits
- Automatically handles timestamp arithmetic to convert relative delays to absolute finish times
- Thread-safe within PostgreSQL single-threaded process model
- Requires the timeout to be previously registered with RegisterTimeout()
- Will overwrite any existing timeout for the same TimeoutId