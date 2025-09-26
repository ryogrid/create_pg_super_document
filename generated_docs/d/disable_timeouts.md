# disable_timeouts

## Location
src/backend/utils/misc/timeout.c: 718 - 750

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
  - find_active_timeout
  - remove_timeout_index
  - GetCurrentTimestamp
  - schedule_alarm
- Types referenced:
  - DisableTimeoutParams
  - TimeoutId
- Called from (representative examples):
  - LockErrorCleanup
  - ProcSleep
  - DisableTimeoutParams

## Notes and Other Information
- Optimizes performance by calling GetCurrentTimestamp() and setitimer() only once regardless of the number of timeouts being disabled
- Each timeout can have its own keep_indicator setting, providing fine-grained control over indicator state preservation
- Includes assertion checks to ensure the timeout system is properly initialized and timeout handlers exist
- Safe to include already-disabled timeouts in the array - they are simply skipped without error
- Particularly useful in lock management and process sleeping scenarios where multiple coordinated timeouts need to be canceled together
- The batch approach is especially beneficial during error cleanup or process termination when multiple timeouts may need to be cleared
- Part of PostgreSQL's advanced timeout management system designed for high-performance concurrent operations