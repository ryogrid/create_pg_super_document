# mxid_to_string

## Location
[src/backend/access/transam/multixact.c:1769-1799](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L1769-L1799)

## Overview
Converts a MultiXact ID and its associated member transactions into a human-readable string representation for debugging and logging purposes.

## Definition

```c
char *
mxid_to_string(MultiXactId multi, int nmembers, MultiXactMember *members)
```
## Detailed Description
This function creates a formatted string representation of a MultiXact ID and its member transactions. The output format is "[MultiXactId] [count][[xid1] ([status1]), [xid2] ([status2]), ...]". The function manages its own static string buffer, freeing the previous result before creating a new one. The resulting string is allocated in TopMemoryContext to ensure it persists across memory context resets, making it suitable for error reporting and debugging scenarios.

## Parameters / Member Variables
- : The MultiXact ID to convert to string
- : The number of member transactions in the MultiXact
- : Array of MultiXactMember structures containing transaction IDs and their status

## Dependencies
- Functions called/Symbols referenced:
  - [mxstatus_to_string](mxstatus_to_string.md)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
  - [initStringInfo](../i/initStringInfo.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [MultiXactIdCreate](../M/MultiXactIdCreate.md)
  - [MultiXactIdCreateFromMembers](../M/MultiXactIdCreateFromMembers.md)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - [mXactCacheGetBySet](mXactCacheGetBySet.md)
  - [mXactCacheGetById](mXactCacheGetById.md)

## Notes and Other Information
- Uses a static buffer that is automatically freed on subsequent calls
- The returned string is allocated in TopMemoryContext for persistence
- Primarily used for debugging, logging, and error reporting
- Format: "MultiXactId count[xid1 (status1), xid2 (status2), ...]"
- Located in src/backend/access/transam/multixact.c:1769-1799