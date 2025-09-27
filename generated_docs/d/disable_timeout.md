# disable_timeout

## Location
[src/backend/utils/misc/timeout.c:685-717](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L685-L717)

## Overview
Cancels a specific timeout, removing it from the active timeout list and optionally resetting its firing indicator.

## Definition

```c
void
disable_timeout(TimeoutId id, bool keep_indicator)
```
## Detailed Description
This function disables a previously enabled timeout by removing it from the active timeout queue. The function provides flexibility in handling the timeout's indicator flag, which tracks whether the timeout has fired. When keep_indicator is false, the indicator is reset, allowing for clean timeout state management. When true, the indicator preserves its state, which can be useful for checking if a timeout fired before being disabled.

The function safely handles the case where a timeout is already disabled (it's not an error), maintains the integrity of other active timeouts, and reschedules the system alarm if other timeouts remain active. This ensures the timeout system continues to function correctly for remaining active timeouts.

## Parameters / Member Variables
- `id`: TimeoutId specifying which timeout to disable
- `keep_indicator`: bool determining whether to preserve the timeout's fired indicator (true) or reset it (false)

## Dependencies
- Functions called/Symbols referenced:
  - disable_alarm
  - [find_active_timeout](../f/find_active_timeout.md)
  - [remove_timeout_index](../r/remove_timeout_index.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [schedule_alarm](../s/schedule_alarm.md)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md)
  - [PrepareTransaction](../P/PrepareTransaction.md)
  - [AbortTransaction](../A/AbortTransaction.md)
  - [disable_startup_progress_timeout](disable_startup_progress_timeout.md)
  - [ProcSleep](../P/ProcSleep.md)
  - [BackendInitialize](../B/BackendInitialize.md)
  - [assign_transaction_timeout](../a/assign_transaction_timeout.md)
  - [PostgresMain](../P/PostgresMain.md)
  - [enable_statement_timeout](../e/enable_statement_timeout.md)
  - [disable_statement_timeout](disable_statement_timeout.md)
  - [PerformAuthentication](../P/PerformAuthentication.md)
  - [DisableTimeoutParams](../D/DisableTimeoutParams.md)

## Notes and Other Information
- Safe to call on timeouts that are already disabled - not considered an error condition
- Includes assertion checks to ensure the timeout system is properly initialized
- Temporarily disables alarm interrupts during the disable operation for thread safety
- Automatically reschedules remaining active timeouts when one is disabled
- The keep_indicator parameter allows for timeout firing detection even after disabling
- Widely used throughout PostgreSQL for transaction management, authentication, statement timeouts, and process control
- Part of PostgreSQL's comprehensive timeout management system for coordinating time-sensitive operations

## Simplified Source

```c
// Simplified version of disable_timeout
void disable_timeout(TimeoutId id, bool keep_indicator) {
    // Safety checks: ensure timeout system is initialized and timeout exists
    Assert(all_timeouts_initialized);
    Assert(all_timeouts[id].timeout_handler != NULL);

    // Step 1: Disable alarm interrupts for thread safety
    disable_alarm();

    // Step 2: Remove timeout from active list if it's currently active
    if (all_timeouts[id].active) {
        int timeout_index = find_active_timeout(id);
        remove_timeout_index(timeout_index);
    }

    // Step 3: Reset the fired indicator unless caller wants to keep it
    if (!keep_indicator) {
        all_timeouts[id].indicator = false;
    }

    // Step 4: Reschedule alarm for remaining active timeouts
    if (num_active_timeouts > 0) {
        schedule_alarm(GetCurrentTimestamp());
    }
}
```

Key simplifications made:
- Combined the find and remove operations into clearer steps
- Added descriptive comments for each major operation
- Emphasized the safety and thread protection aspects
- Maintained the essential logic flow while making it more readable
- Clarified the purpose of the keep_indicator parameter