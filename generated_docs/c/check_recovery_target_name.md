# check_recovery_target_name

## Location
src/backend/access/transam/xlogrecovery.c: 4854 - 4869

## Overview
A GUC (Grand Unified Configuration) check hook function that validates the `recovery_target_name` parameter, ensuring the restore point name doesn't exceed the maximum allowed filename length.

## Definition
```c
bool check_recovery_target_name(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function serves as a validation hook for the `recovery_target_name` PostgreSQL configuration parameter. When a user attempts to set this parameter to specify a named restore point for recovery, PostgreSQL calls this function to verify that the provided name is valid. The function performs a simple length check against `MAXFNAMELEN` to ensure the restore point name won't cause filesystem-related issues. Unlike LSN or time-based recovery targets, named recovery targets don't require complex parsing, so this function only validates the string length. If the name is too long, it returns false and provides a detailed error message using the GUC error reporting mechanism.

## Parameters / Member Variables
- `newval`: Pointer to the new string value being assigned to the GUC parameter (restore point name)
- `extra`: Pointer for storing additional data (unused in this function as no parsing is required)
- `source`: The source of the GUC setting (e.g., configuration file, command line, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - GUC_check_errdetail (provides detailed error messages for GUC validation failures)
  - MAXFNAMELEN (maximum filename length constant)
  - GucSource (enum type for configuration source)
- Called from (representative examples):
  - PostgreSQL GUC system when recovery_target_name parameter is being set

## Notes and Other Information
- This is part of PostgreSQL's point-in-time recovery (PITR) system for named restore points
- Named restore points are created using `pg_create_restore_point()` function during normal operation
- The length limit prevents filesystem issues when working with restore point names
- Unlike other recovery target check hooks, this function doesn't allocate extra data since no parsing is required
- The function allows empty strings, enabling users to unset the recovery target name
- Restore point names are case-sensitive and must exactly match the name used when creating the restore point