# check_recovery_target

## Location
src/backend/access/transam/xlogrecovery.c: 4782 - 4795

## Overview
check_recovery_target is a GUC check hook function that validates the recovery_target configuration parameter, ensuring it only accepts "immediate" or empty string values.

## Definition
```c
bool check_recovery_target(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function serves as a validation hook for the recovery_target GUC parameter in PostgreSQL's point-in-time recovery system. The recovery_target parameter controls when recovery should stop during WAL replay. This particular hook enforces that the parameter can only be set to:

1. "immediate" - Recovery stops as soon as a consistent state is reached
2. "" (empty string) - Unsets any recovery target, allowing other recovery_target_* parameters to be used

Any other value is rejected with a specific error message. This strict validation prevents invalid recovery target specifications that could lead to unexpected recovery behavior.

## Parameters / Member Variables
- `newval`: Double pointer to the new string value being assigned to recovery_target (can be modified by the hook)
- `extra`: Double pointer for storing additional data that may be used by assign hooks (unused in this function)
- `source`: Indicates the source of the configuration change (e.g., config file, command line, SQL SET command)

## Dependencies
- Functions called/Symbols referenced:
  - GUC_check_errdetail (for error reporting)
  - GucSource (enum type)
- Called from:
  - PostgreSQL GUC system (registered as check hook in guc_hooks.h)

## Notes and Other Information
- Returns true if validation passes, false if the new value should be rejected
- Part of PostgreSQL's recovery target system where only one recovery_target_* parameter may be active at a time
- The "immediate" option is used for standby servers that want to become available as soon as they reach consistency
- Empty string allows unsetting this parameter to use other recovery target types (LSN, name, time, XID)
- Error messages are provided through GUC_check_errdetail for user feedback
- This hook prevents configuration conflicts in PostgreSQL's point-in-time recovery mechanism