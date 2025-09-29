# TempTablespacesAreSet

## Location
[src/backend/storage/file/fd.c:3075-3089](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3075-L3089)

## Overview
Returns a boolean indicating whether temporary tablespaces have been configured for the current transaction via SetTempTablespaces.

## Definition
```c
bool TempTablespacesAreSet(void)
```

## Detailed Description
The `TempTablespacesAreSet` function provides a simple check to determine if temporary tablespaces have been configured for the current transaction. It serves as a state query function that allows other parts of the system, particularly the tablespace management code, to determine whether custom temporary tablespace configuration is active.

The function works by checking the global variable `numTempTableSpaces`, which is initialized to -1 at system startup and set to a non-negative value when `SetTempTablespaces` is called. This design eliminates the need for tablespace.c to maintain its own per-transaction state tracking.

This is a lightweight utility function that enables proper coordination between the file descriptor management system and the tablespace management system.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - None (direct access to global variable)

- Global variables accessed:
  - `numTempTableSpaces` - Count of configured temporary tablespaces

- Called from (representative examples):
  - `[PrepareTempTablespaces](../P/PrepareTempTablespaces.md)` (src/backend/commands/tablespace.c:1340)
  - `[GetTempTablespaces](../G/GetTempTablespaces.md)` (src/backend/storage/file/fd.c:3094)

## Notes and Other Information
- This function is designed to avoid the need for tablespace.c to maintain its own per-transaction state
- The check is based on `numTempTableSpaces >= 0`, where -1 indicates uninitialized/unset state
- Very lightweight operation with minimal computational overhead
- Essential for proper coordination between file descriptor and tablespace management systems
- The function enables conditional logic in tablespace preparation and selection routines

## Simplified Source

```c
bool
TempTablespacesAreSet(void)
{
    // Check if temp tablespaces have been configured
    // (numTempTableSpaces is -1 when unset, >= 0 when configured)
    return (numTempTableSpaces >= 0);
}
```