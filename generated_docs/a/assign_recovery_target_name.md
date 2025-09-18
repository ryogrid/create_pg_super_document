# assign_recovery_target_name

## Location
src/backend/access/transam/xlogrecovery.c: 4870 - 4894

## Overview
A GUC (Grand Unified Configuration) assign hook function that applies the validated `recovery_target_name` parameter value, setting the recovery target type and restore point name when the configuration change is committed.

## Definition
```c
void assign_recovery_target_name(const char *newval, void *extra)
```

## Detailed Description
This function serves as the assignment hook for the `recovery_target_name` PostgreSQL configuration parameter. It is called after the corresponding check hook has validated the input value. The function ensures that only one recovery target can be set at a time by checking if another recovery target is already configured and calling `error_multiple_recovery_targets()` if there's a conflict. When a valid restore point name is provided (non-empty string), it sets the global recovery target type to `RECOVERY_TARGET_NAME` and stores the restore point name directly in the global variable. If an empty string is provided, it unsets the recovery target. Unlike LSN-based recovery targets, named targets don't require parsed data since the string name is used directly.

## Parameters / Member Variables
- `newval`: The new string value being assigned to the GUC parameter (restore point name, validated by check hook)
- `extra`: Additional data (unused in this function since no parsing is required for names)

## Dependencies
- Functions called/Symbols referenced:
  - error_multiple_recovery_targets (prevents multiple recovery targets from being set)
  - RECOVERY_TARGET_UNSET (enum value for no recovery target)
  - RECOVERY_TARGET_NAME (enum value for named restore point recovery target)
- Called from (representative examples):
  - PostgreSQL GUC system when recovery_target_name parameter assignment is finalized

## Notes and Other Information
- This is part of PostgreSQL's point-in-time recovery (PITR) system for named restore points
- Works in conjunction with `check_recovery_target_name` to provide complete parameter validation and assignment
- Sets global variables `recoveryTarget` and `recoveryTargetName` that are used during recovery processing
- Enforces mutual exclusivity with other recovery target types (LSN, time, XID, etc.)
- The restore point name is stored directly without copying, relying on GUC system memory management
- Named restore points are created during normal database operation using `pg_create_restore_point()`
- Empty string values result in unsetting the recovery target rather than an error
- The restore point name must exactly match a previously created restore point for recovery to succeed