# assign_recovery_target_time

## Location
[src/backend/access/transam/xlogrecovery.c:4950-4965](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4950-L4965)

## Overview
A GUC assign hook function that sets the recovery target to a specific timestamp, ensuring only one recovery target type is active at a time.

## Definition

```c
void
assign_recovery_target_time(const char *newval, void *extra)
```
## Detailed Description
This function serves as a GUC (Grand Unified Configuration) assign hook for the  parameter. It validates and sets the recovery target type to  when a timestamp value is provided, or resets it to  when the parameter is cleared. The function enforces mutual exclusivity among different recovery target types by calling  if another recovery target type is already set.

## Parameters / Member Variables
- `*newval`: The new value being assigned to the  parameter (timestamp string or empty)
- `*extra`: Additional data passed by the GUC system (unused in this function)
## Dependencies
- Functions called/Symbols referenced:
  - error_multiple_recovery_targets (when multiple targets are detected)
  - RECOVERY_TARGET_UNSET (enum value)
  - RECOVERY_TARGET_TIME (enum value)
- Called from (representative examples):
  - GUC system (via function pointer in GUC_HOOKS_H)

## Notes and Other Information
- This function is part of PostgreSQL's point-in-time recovery (PITR) system
- It ensures that only one type of recovery target can be active at any given time
- The function is registered as a GUC assign hook and called automatically when the recovery_target_time configuration parameter is set
- Located in src/backend/access/transam/xlogrecovery.c:4950-4965

## Simplified Source

```c
void assign_recovery_target_time(const char *newval, void *extra) {
    // Check for conflicts with other recovery target types
    if (recoveryTarget != RECOVERY_TARGET_UNSET &&
        recoveryTarget != RECOVERY_TARGET_TIME)
        error_multiple_recovery_targets();

    // Set or unset time-based recovery target
    if (newval && strcmp(newval, "") != 0)
        recoveryTarget = RECOVERY_TARGET_TIME;  // Enable time-based recovery
    else
        recoveryTarget = RECOVERY_TARGET_UNSET; // Unset recovery target
}
```