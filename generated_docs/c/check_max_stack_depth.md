# check_max_stack_depth

## Location
[src/backend/tcop/postgres.c:3605-3621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3605-L3621)

## Overview
A GUC (Grand Unified Configuration) check hook function that validates the max_stack_depth configuration parameter to ensure it doesn't exceed the platform's stack limit.

## Definition

```c
bool
check_max_stack_depth(int *newval, void **extra, GucSource source)
```
## Detailed Description
This function serves as a validation hook for the PostgreSQL configuration parameter max_stack_depth. It converts the proposed value from kilobytes to bytes and compares it against the system's stack depth limit obtained from get_stack_depth_rlimit(). The function ensures that the requested stack depth doesn't exceed the available system limit minus a safety margin (STACK_DEPTH_SLOP), preventing potential stack overflow conditions.

If the validation fails, the function provides detailed error messages to help administrators understand the issue and suggests increasing the platform's stack depth limit using ulimit -s.

## Parameters / Member Variables
- : Pointer to the new value being set for max_stack_depth (in kilobytes)
- : Pointer to extra data (unused in this function)
- : The source of the configuration change (GucSource enumeration)

## Dependencies
- Functions called/Symbols referenced:
  - get_stack_depth_rlimit
  - GUC_check_errdetail
  - GUC_check_errhint
  - STACK_DEPTH_SLOP
- Called from (representative examples):
  - Referenced in GUC_HOOKS_H (src/include/utils/guc_hooks.h:92)

## Notes and Other Information
- This function is part of PostgreSQL's GUC (Grand Unified Configuration) system
- The function includes a safety margin (STACK_DEPTH_SLOP) to prevent stack overflow
- Returns true if the validation passes, false if the proposed value is too large
- Provides helpful error messages and hints when validation fails
- The validation only occurs if a positive stack limit is available from the system