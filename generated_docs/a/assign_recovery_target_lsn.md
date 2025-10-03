# assign_recovery_target_lsn

## Location
[src/backend/access/transam/xlogrecovery.c:4835-4853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4835-L4853)

## Overview
A GUC (Grand Unified Configuration) assign hook function that applies the validated  parameter value, setting the recovery target type and LSN when the configuration change is committed.

## Definition
```c
void assign_recovery_target_lsn(const char *newval, void *extra)
```

## Detailed Description
This function serves as the assignment hook for the  PostgreSQL configuration parameter. It is called after the corresponding check hook has validated the input value. The function ensures that only one recovery target can be set at a time by checking if another recovery target is already configured and calling  if there's a conflict. When a valid LSN value is provided (non-empty string), it sets the global recovery target type to  and stores the parsed LSN value from the extra data. If an empty string is provided, it unsets the recovery target.

## Parameters / Member Variables
- `*newval`: The new string value being assigned to the GUC parameter (validated by check hook)
- `*extra`: Additional data containing the pre-parsed XLogRecPtr LSN value from the check hook
## Dependencies
- Functions called/Symbols referenced:
  - error_multiple_recovery_targets (prevents multiple recovery targets from being set)
  - RECOVERY_TARGET_UNSET (enum value for no recovery target)
  - RECOVERY_TARGET_LSN (enum value for LSN-based recovery target)
- Called from (representative examples):
  - PostgreSQL GUC system when recovery_target_lsn parameter assignment is finalized

## Notes and Other Information
- This is part of PostgreSQL's point-in-time recovery (PITR) system
- Works in conjunction with  to provide complete parameter validation and assignment
- Sets global variables  and  that are used during recovery processing
- Enforces mutual exclusivity with other recovery target types (time, name, XID, etc.)
- The function assumes the LSN has already been validated by the check hook
- Empty string values result in unsetting the recovery target rather than an error