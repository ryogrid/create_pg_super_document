# assign_recovery_target_xid

## Location
[src/backend/access/transam/xlogrecovery.c:5035-5048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L5035-L5048)

## Overview
A GUC assign hook function that sets the recovery target to a specific transaction ID, ensuring mutual exclusivity with other recovery target types.

## Definition

```c
void
assign_recovery_target_xid(const char *newval, void *extra)
```
## Detailed Description
This function serves as a GUC assign hook for the  parameter. It validates that no other recovery target type is currently set and then configures the recovery system to stop at a specific transaction ID. When a valid XID is provided, it sets the recovery target type to  and stores the transaction ID in . When the parameter is cleared (empty string), it resets the recovery target to unset. The function enforces mutual exclusivity among different recovery target types by calling  when conflicts are detected.

## Parameters / Member Variables
- `*newval`: The new value string for recovery_target_xid (transaction ID or empty string)
- `*extra`: Pointer to the validated TransactionId from the check hook
## Dependencies
- Functions called/Symbols referenced:
  - error_multiple_recovery_targets (when multiple targets are detected)
  - RECOVERY_TARGET_UNSET (enum value)
  - RECOVERY_TARGET_XID (enum value)
  - recoveryTarget (global variable)
  - recoveryTargetXid (global variable)
- Called from (representative examples):
  - GUC system (via function pointer in GUC_HOOKS_H)

## Notes and Other Information
- This function is part of PostgreSQL's point-in-time recovery (PITR) system
- Ensures that only one type of recovery target can be active at any given time
- The TransactionId value has already been validated by check_recovery_target_xid
- Sets global recovery state variables that guide WAL replay to stop at the specified transaction
- Located in src/backend/access/transam/xlogrecovery.c:5035-5048