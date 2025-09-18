# assign_recovery_target

## Location
src/backend/access/transam/xlogrecovery.c: 4796 - 4811

## Overview
assign_recovery_target is a GUC assign hook function that sets the global recovery target state when the recovery_target parameter is changed, ensuring mutual exclusivity among recovery target options.

## Definition
```c
void assign_recovery_target(const char *newval, void *extra)
```

## Detailed Description
This function serves as an assignment hook for the recovery_target GUC parameter in PostgreSQL's point-in-time recovery system. It manages the global recoveryTarget variable that tracks which type of recovery target is currently active. The function enforces the rule that only one recovery target type may be set at a time across all recovery_target_* parameters.

Key behaviors:
1. **Conflict Detection**: Checks if another recovery target is already set and calls error_multiple_recovery_targets() if so
2. **State Management**: Sets recoveryTarget to RECOVERY_TARGET_IMMEDIATE when "immediate" is specified
3. **Unsetting**: Resets recoveryTarget to RECOVERY_TARGET_UNSET when an empty string is provided

This ensures that recovery targets are mutually exclusive and prevents configuration conflicts that could lead to ambiguous recovery behavior.

## Parameters / Member Variables
- `newval`: The new string value being assigned to recovery_target ("immediate" or "" for unset)
- `extra`: Additional data from the check hook (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - error_multiple_recovery_targets (called when conflicts detected)
  - RECOVERY_TARGET_UNSET (enum value)
  - RECOVERY_TARGET_IMMEDIATE (enum value)
- Global variables accessed:
  - recoveryTarget (global recovery target state tracker)
- Called from:
  - PostgreSQL GUC system (registered as assign hook in guc_hooks.h)

## Notes and Other Information
- This hook runs after successful validation by check_recovery_target
- The function enforces PostgreSQL's "only one recovery target" rule across all recovery_target_* parameters
- error_multiple_recovery_targets() is marked with pg_attribute_noreturn() and terminates with FATAL error
- Part of a coordinated system where similar assign hooks exist for recovery_target_lsn, recovery_target_name, recovery_target_time, and recovery_target_xid
- The recoveryTarget global variable is used throughout the recovery process to determine stopping conditions
- Empty string assignment allows switching to different recovery target types after unsetting the current one