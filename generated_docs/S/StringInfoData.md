# StringInfoData

## Location
[src/include/lib/stringinfo.h:46-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/stringinfo.h#L46-L52)

## Overview
StringInfoData is a fundamental data structure in PostgreSQL that holds information about an extensible string buffer, providing dynamic string management capabilities with optional read-only mode.

## Definition

```c
typedef struct StringInfoData
{
	char	   *data;
	int			len;
	int			maxlen;
	int			cursor;
} StringInfoData;
```
## Detailed Description
StringInfoData serves as PostgreSQL's primary dynamic string buffer implementation. It supports both mutable strings that can be extended through various append operations and read-only strings for performance-critical scenarios where copying would be too costly. The structure maintains metadata about the buffer's current state, allocated size, and provides a cursor for scanning operations.

The structure supports two operational modes:
1. **Normal mode**:  points to a palloc'd buffer that can be reallocated as needed, with  indicating the allocated size
2. **Read-only mode**:  points to an external buffer managed by the caller, with  set to 0 to indicate read-only status

## Parameters / Member Variables
- : Pointer to the current string buffer containing the actual string data
- : Current length of the string content (excluding null terminator in normal mode)
- : Allocated size of the buffer in bytes; set to 0 for read-only strings to indicate special handling
- : Position indicator initialized to zero, used by scanning routines but not modified by core stringinfo functions

## Dependencies
- Functions called/Symbols referenced: (None directly)
- Called from (representative examples):
  - [initReadOnlyStringInfo](../i/initReadOnlyStringInfo.md) (initializes read-only instances)
  - [initStringInfoFromString](../i/initStringInfoFromString.md) (initializes from existing string)
  - makeStringInfo (creates new instances)
  - appendStringInfo family of functions

## Notes and Other Information
- In normal mode, a terminating null character is guaranteed at 
- Read-only strings may or may not have null termination, depending on intended usage
- The constraint  must always hold except in read-only mode
- Read-only StringInfoData instances cannot be used with append or reset operations
- The cursor field is available for application-specific scanning but is not used by the core stringinfo routines