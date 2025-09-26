# GetLockNameFromTagType

## Location
[src/backend/storage/lmgr/lmgr.c:1336-1341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L1336-L1341)

## Overview
Returns the string name corresponding to a given lock tag type, providing a human-readable identifier for different types of lockable objects.

## Definition
```c
const char *GetLockNameFromTagType(uint16 locktag_type)
```

## Detailed Description
This function provides a simple mapping from numeric lock tag types to their corresponding string names. It performs bounds checking to ensure the provided lock tag type is valid (not exceeding LOCKTAG_LAST_TYPE) and returns "???" for invalid types. For valid types, it returns the appropriate string from the LockTagTypeNames array, which contains names like "relation", "page", "tuple", "transactionid", etc. This function is primarily used for monitoring and debugging purposes where human-readable lock type names are needed.

## Parameters / Member Variables
- `locktag_type`: The numeric lock tag type identifier to convert to a string name

## Dependencies
- Functions called/Symbols referenced:
  - LOCKTAG_LAST_TYPE (constant defining the maximum valid lock tag type)
  - LockTagTypeNames (array of string names corresponding to each lock tag type)
- Called from (representative examples):
  - pgstat_get_wait_event (for wait event reporting in pg_stat_activity)

## Notes and Other Information
- Returns a const char pointer to a static string, so the result should not be modified or freed
- The LockTagTypeNames array contains entries for all lock types: "relation", "extend", "frozenid", "page", "tuple", "transactionid", "virtualxid", "spectoken", "object", "userlock", "advisory", "applytransaction"
- Includes bounds checking to prevent array access violations
- The "???" return value for invalid types helps with debugging when unexpected lock tag types are encountered
- Used in PostgreSQL's monitoring infrastructure to provide meaningful names in views like pg_locks and wait event reporting