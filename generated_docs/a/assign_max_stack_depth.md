# assign_max_stack_depth

## Location
[src/backend/tcop/postgres.c:3622-3632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3622-L3632)

## Overview
A GUC (Grand Unified Configuration) assign hook function that sets the global max_stack_depth_bytes variable when the max_stack_depth configuration parameter is changed.

## Definition

```c
void
assign_max_stack_depth(int newval, void *extra)
```
## Detailed Description
This function serves as an assignment hook for the PostgreSQL configuration parameter max_stack_depth. It is called by the GUC system whenever the max_stack_depth parameter is successfully validated and needs to be applied. The function converts the value from kilobytes to bytes and stores it in the global variable max_stack_depth_bytes, which is used throughout PostgreSQL for stack depth checking and management.

This hook works in conjunction with check_max_stack_depth to ensure that stack depth limits are properly validated before being applied to the system.

## Parameters / Member Variables
- `newval`: The new value for max_stack_depth (in kilobytes) that has been validated
- `*extra`: Pointer to extra data (unused in this function)
## Dependencies
- Functions called/Symbols referenced:
  - max_stack_depth_bytes (global variable assignment)
- Called from (representative examples):
  - Referenced in GUC_HOOKS_H (src/include/utils/guc_hooks.h:93)

## Notes and Other Information
- This function is part of PostgreSQL's GUC (Grand Unified Configuration) system
- It is called only after the value has been successfully validated by check_max_stack_depth
- The function performs a simple unit conversion from kilobytes to bytes
- The resulting value is stored in max_stack_depth_bytes for use in stack depth monitoring
- This is a void function as it always succeeds (validation occurs in the check hook)

## Simplified Source

```c
void assign_max_stack_depth(int newval, void *extra) {
    // Convert kilobytes to bytes and store in global variable
    // Called by GUC system when max_stack_depth is updated
    max_stack_depth_bytes = newval * 1024L;
}
```