# disable_timeouts

## Location
[src/backend/utils/misc/timeout.c:718-750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L718-L750)

## Overview
Cancels multiple timeouts simultaneously, optimizing performance by reducing system calls when disabling multiple timeouts at once.

## Definition

```c
void
disable_timeouts(const DisableTimeoutParams *timeouts, int count)
```
## Detailed Description
This function provides an efficient way to disable multiple timeouts in a single operation, avoiding repeated calls to GetCurrentTimestamp() and setitimer() that would occur when canceling timeouts individually. It processes each timeout in the provided array, removing active timeouts from the timeout queue and optionally resetting their firing indicators based on the keep_indicator flag for each timeout.

The function follows the same safety principles as disable_timeout() but applies them in batch mode. It temporarily disables alarm interrupts during the entire operation, processes all requested timeouts, and then reschedules the system alarm only once if any timeouts remain active. This batch processing approach significantly improves performance when multiple timeouts need to be canceled simultaneously.

## Parameters / Member Variables
- `timeouts`: const DisableTimeoutParams* pointing to an array of timeout parameter structures specifying which timeouts to disable
- `count`: int specifying the number of timeouts to disable from the array

## Dependencies
- Functions called/Symbols referenced:
  - disable_alarm
  - [find_active_timeout](../f/find_active_timeout.md)
  - [remove_timeout_index](../r/remove_timeout_index.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [schedule_alarm](../s/schedule_alarm.md)
- Types referenced:
  - [DisableTimeoutParams](../D/DisableTimeoutParams.md)
  - [TimeoutId](../T/TimeoutId.md)
- Called from (representative examples):
  - [LockErrorCleanup](../L/LockErrorCleanup.md)
  - [ProcSleep](../P/ProcSleep.md)
  - [DisableTimeoutParams](../D/DisableTimeoutParams.md)

## Notes and Other Information
- Optimizes performance by calling GetCurrentTimestamp() and setitimer() only once regardless of the number of timeouts being disabled
- Each timeout can have its own keep_indicator setting, providing fine-grained control over indicator state preservation
- Includes assertion checks to ensure the timeout system is properly initialized and timeout handlers exist
- Safe to include already-disabled timeouts in the array - they are simply skipped without error
- Particularly useful in lock management and process sleeping scenarios where multiple coordinated timeouts need to be canceled together
- The batch approach is especially beneficial during error cleanup or process termination when multiple timeouts may need to be cleared
- Part of PostgreSQL's advanced timeout management system designed for high-performance concurrent operations

## Simplified Source

```c
// Simplified version of disable_timeouts
void disable_timeouts(const DisableTimeoutParams *timeouts, int count) {
    // Safety check: ensure timeout system is initialized
    Assert(all_timeouts_initialized);

    // Disable interrupts to prevent race conditions during batch operation
    disable_alarm();

    // Process each timeout in the batch
    for (int i = 0; i < count; i++) {
        TimeoutId id = timeouts[i].id;

        // Verify timeout is properly configured
        Assert(all_timeouts[id].timeout_handler != NULL);

        // Remove from active queue if currently active
        if (all_timeouts[id].active) {
            remove_timeout_index(find_active_timeout(id));
        }

        // Clear indicator unless caller wants to preserve it
        if (!timeouts[i].keep_indicator) {
            all_timeouts[id].indicator = false;
        }
    }

    // Re-enable alarms if any timeouts still remain active
    if (num_active_timeouts > 0) {
        schedule_alarm(GetCurrentTimestamp());
    }
}
```

Key simplifications made:
- Removed detailed comments and consolidated them into clear step descriptions
- Simplified variable declarations (moved loop variable inline)
- Added high-level comments explaining the purpose of each major step
- Preserved all essential logic including safety checks and batch processing
- Maintained the core algorithm: disable interrupts → process timeouts → reschedule if needed
- Kept assertion checks as they're critical for debugging timeout system issues