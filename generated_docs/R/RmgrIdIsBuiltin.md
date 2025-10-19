# RmgrIdIsBuiltin

## Location
[src/include/access/rmgr.h:42-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/rmgr.h#L42-L47)

## Overview
Determines whether a resource manager ID corresponds to a built-in PostgreSQL resource manager.

## Definition

```c
static inline bool
RmgrIdIsBuiltin(int rmid)
```
## Detailed Description
This inline function checks if a given resource manager ID (rmid) is a built-in PostgreSQL resource manager. Built-in resource managers are those that are part of the core PostgreSQL system, as opposed to custom resource managers that can be added by extensions. The function performs a simple comparison against RM_MAX_BUILTIN_ID, which represents the highest ID assigned to built-in resource managers.

Resource managers in PostgreSQL are responsible for handling different types of WAL (Write-Ahead Logging) records. Built-in resource managers handle core PostgreSQL operations like heap operations, btree operations, hash operations, etc.

## Parameters / Member Variables
- `rmid`: The resource manager ID to check (integer value)

## Dependencies
- Functions called/Symbols referenced:
  - RM_MAX_BUILTIN_ID (constant defining the maximum built-in resource manager ID)
- Called from (representative examples):
  - PG_GET_RESOURCE_MANAGERS_COLS (in rmgr.c)
  - [GetRmgrDesc](../G/GetRmgrDesc.md) (in pg_waldump/rmgrdesc.c)
  - RmgrIdIsValid (macro in rmgr.h)

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Built-in resource manager IDs range from 0 to RM_MAX_BUILTIN_ID (RM_NEXT_ID - 1)
- Custom resource manager IDs start from RM_MIN_CUSTOM_ID (128) and go up to RM_MAX_CUSTOM_ID (255)
- Used primarily for validation and categorization of resource manager IDs in WAL processing
- The function is complementary to RmgrIdIsCustom() for complete coverage of valid resource manager ID ranges

## Simplified Source

```c
static inline bool
RmgrIdIsBuiltin(int rmid)
{
    // Check if resource manager ID is within built-in range
    return rmid <= RM_MAX_BUILTIN_ID;
}
```