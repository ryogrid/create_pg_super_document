# GetLockmodeName

## Location
[src/backend/storage/lmgr/lock.c:4070-4083](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L4070-L4083)

## Overview
GetLockmodeName provides the textual name of any lock mode given a lock method ID and mode number, serving as a utility function for debugging and user-facing lock information display.

## Definition

```c
const char *
GetLockmodeName(LOCKMETHODID lockmethodid, LOCKMODE mode)
```
## Detailed Description
This simple utility function translates numeric lock mode identifiers into human-readable string names. It accesses the global LockMethods array to retrieve the appropriate lock mode name from the specified lock method's configuration.

The function performs bounds checking to ensure both the lock method ID and mode number are valid before accessing the lock mode names array. This prevents potential crashes from invalid parameters while providing a clean interface for lock mode name lookup.

PostgreSQL supports different lock methods (though DEFAULT_LOCKMETHOD is most commonly used), and each lock method can have different lock modes with different names. This function provides a method-agnostic way to get the textual representation of any lock mode.

## Parameters / Member Variables
- : The lock method identifier (typically DEFAULT_LOCKMETHOD)
- : The lock mode number to get the name for

**Return value**:  - A string containing the lock mode name (e.g., "AccessShareLock", "RowExclusiveLock", "AccessExclusiveLock")

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to determine array length
  -  - Global array of lock method configurations
  -  - Debug assertion checking

- Called from (representative examples):
  -  - Deadlock reporting and logging
  -  - Lock wait logging and debugging
  -  - SQL function for displaying lock information

## Notes and Other Information
- The function is simple but critical for debugging and monitoring lock behavior
- Both parameters are validated with assertions to catch programming errors
- Returns a pointer to a static string stored in the lock method configuration
- The returned string should not be modified by callers
- Commonly used lock mode names include: "AccessShareLock", "RowShareLock", "RowExclusiveLock", "ShareUpdateExclusiveLock", "ShareLock", "ShareRowExclusiveLock", "ExclusiveLock", "AccessExclusiveLock"
- Essential for user-visible lock reporting in system views and functions
- The function assumes the caller has already validated that the lock method and mode are legitimate