# assign_wal_consistency_checking

## Location
[src/backend/access/transam/xlog.c:4712-4738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L4712-L4738)

## Overview
A GUC assign hook function that assigns the validated wal_consistency_checking configuration to the global variable after successful validation.

## Definition
```c
void assign_wal_consistency_checking(const char *newval, void *extra)
```

## Detailed Description
This function serves as the assign hook for the wal_consistency_checking GUC parameter. It is called after successful validation by check_wal_consistency_checking() to actually apply the new configuration. The function simply assigns the pre-validated extra data (boolean array) to the global wal_consistency_checking variable.

The function includes important comments about the timing of assignments: built-in resource managers are assigned immediately and affect WAL created before shared_preload_libraries are processed, while custom resource managers are assigned later but this is acceptable since custom WAL cannot be written before the modules are loaded.

## Parameters / Member Variables
- `newval`: The new string value (unused in this function)
- `extra`: Pre-validated boolean array from the check hook

## Dependencies
- Functions called/Symbols referenced:
  - None (simple assignment)
- Called from (representative examples):
  - GUC system during parameter assignment

## Notes and Other Information
- This is a simple assignment function with no validation logic
- The actual validation was performed by check_wal_consistency_checking()
- Built-in resource managers are assigned immediately upon configuration change
- Custom resource managers may have deferred assignment until modules are loaded
- The function handles the case where some checks were deferred during startup