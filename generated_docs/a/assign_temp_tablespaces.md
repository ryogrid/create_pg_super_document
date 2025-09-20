# assign_temp_tablespaces

## Location
[src/backend/commands/tablespace.c:1306-1330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L1306-L1330)

## Overview
Applies validated temp_tablespaces GUC settings by passing the list of tablespace OIDs to the storage manager's temporary file handling subsystem.

## Definition

```c
void
assign_temp_tablespaces(const char *newval, void *extra)
```
## Detailed Description
This function serves as an assign hook for the temp_tablespaces GUC variable. It takes the validated tablespace information prepared by check_temp_tablespaces and communicates it to the file descriptor (fd.c) subsystem that manages temporary file placement.

The function handles two scenarios:
1. **Valid extra data**: When check_temp_tablespaces was executed inside a transaction and successfully validated tablespaces, it passes the tablespace OID array to SetTempTablespaces
2. **No extra data**: When validation couldn't be performed (outside transaction) or during transaction cleanup, it clears the temporary tablespace list, allowing the next PrepareTempTablespaces call to reinitialize properly

This design ensures that temporary file placement always uses validated, accessible tablespaces while gracefully handling edge cases like transaction boundaries and system restoration scenarios.

## Parameters
- : The new string value for temp_tablespaces GUC (not directly used in this function)
- : Pointer to temp_tablespaces_extra structure containing validated tablespace OIDs from check_temp_tablespaces

## Dependencies
- Functions called/Symbols referenced:
  - SetTempTablespaces
  - temp_tablespaces_extra (struct type)
- Called from (representative examples):
  - GUC system (referenced in src/include/utils/guc_hooks.h:159)

## Notes and Other Information
- This function is always called after check_temp_tablespaces during GUC variable assignment
- The extra parameter may be NULL if validation couldn't be performed (e.g., outside a transaction)
- When extra is NULL, the function clears the temporary tablespace list as a safety measure
- The actual tablespace OID array and count are passed via the temp_tablespaces_extra structure
- Works in conjunction with PrepareTempTablespaces to ensure temporary files are placed in appropriate tablespaces