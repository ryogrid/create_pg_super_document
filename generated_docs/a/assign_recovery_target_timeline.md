# assign_recovery_target_timeline

## Location
[src/backend/access/transam/xlogrecovery.c:4999-5011](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4999-L5011)

## Overview
A GUC assign hook function that sets the recovery target timeline goal and specific timeline ID based on validated input from the check hook.

## Definition

```c
void
assign_recovery_target_timeline(const char *newval, void *extra)
```
## Detailed Description
This function serves as a GUC assign hook for the  parameter. It receives the validated timeline goal type from the check hook via the  parameter and sets the global  variable accordingly. For numeric timeline specifications, it also parses and stores the specific timeline ID in . For non-numeric goals ("current" or "latest"), the requested timeline ID is set to 0, indicating that the actual timeline will be determined dynamically during recovery.

## Parameters / Member Variables
- : The new value string for recovery_target_timeline (timestamp string or keyword)
- : Pointer to RecoveryTargetTimeLineGoal enum value set by the check hook

## Dependencies
- Functions called/Symbols referenced:
  - strtoul (for numeric timeline ID conversion)
  - RecoveryTargetTimeLineGoal (enum type)
  - RECOVERY_TARGET_TIMELINE_NUMERIC (enum value)
  - recoveryTargetTimeLineGoal (global variable)
  - recoveryTargetTLIRequested (global variable)
- Called from (representative examples):
  - GUC system (via function pointer in GUC_HOOKS_H)

## Notes and Other Information
- This function is part of PostgreSQL's point-in-time recovery (PITR) system
- Works in conjunction with check_recovery_target_timeline to validate and process timeline specifications
- Sets global recovery state variables that guide the timeline selection during WAL replay
- The numeric conversion is safe here since validation was already performed in the check hook
- Located in src/backend/access/transam/xlogrecovery.c:4999-5011