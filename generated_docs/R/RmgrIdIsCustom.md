# RmgrIdIsCustom

## Location
[src/include/access/rmgr.h:48-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/rmgr.h#L48-L52)

## Overview
Determines whether a resource manager ID corresponds to a custom (extension-defined) resource manager.

## Definition
```c
static inline bool RmgrIdIsCustom(int rmid)
```

## Detailed Description
This inline function checks if a given resource manager ID (rmid) falls within the range reserved for custom resource managers. Custom resource managers are those implemented by PostgreSQL extensions, as opposed to the built-in resource managers that are part of the core PostgreSQL system.

The function validates that the ID is within the valid custom range, from RM_MIN_CUSTOM_ID (128) to RM_MAX_CUSTOM_ID (255). This range provides space for up to 128 custom resource managers that can be registered by extensions to handle their own WAL record types.

Custom resource managers allow extensions to participate in PostgreSQL's Write-Ahead Logging system by defining their own record types and recovery procedures.

## Parameters / Member Variables
- `rmid`: The resource manager ID to check (integer value)

## Dependencies
- Functions called/Symbols referenced:
  - RM_MIN_CUSTOM_ID (constant defining minimum custom resource manager ID, value 128)
  - RM_MAX_CUSTOM_ID (constant defining maximum custom resource manager ID, value UINT8_MAX/255)
- Called from (representative examples):
  - [RegisterCustomRmgr](RegisterCustomRmgr.md) (in rmgr.c)
  - [XLogDumpDisplayStats](../X/XLogDumpDisplayStats.md) (in pg_waldump.c)
  - [main](../m/main.md) (in pg_waldump.c)
  - RmgrIdIsValid (macro in rmgr.h)

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Custom resource manager IDs range from 128 to 255, providing 128 possible custom resource managers
- The function performs a range check with both lower and upper bounds
- Used for validation when registering custom resource managers and during WAL processing
- Complementary to RmgrIdIsBuiltin() to provide complete coverage of valid resource manager ID ranges
- Extensions must reserve unique IDs from the PostgreSQL community to avoid conflicts
- RM_EXPERIMENTAL_ID (128) is reserved for development/testing purposes

## Simplified Source

```c
static inline bool
RmgrIdIsCustom(int rmid)
{
    // Check if resource manager ID is within custom extension range (128-255)
    return rmid >= RM_MIN_CUSTOM_ID && rmid <= RM_MAX_CUSTOM_ID;
}
```