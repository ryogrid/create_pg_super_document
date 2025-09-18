# mxid_to_string

## Location
src/backend/access/transam/multixact.c: 1769 - 1799

## Overview
Converts a MultiXact ID and its associated member transactions into a human-readable string representation for debugging and logging purposes.

## Definition


## Detailed Description
This function creates a formatted string representation of a MultiXact ID and its member transactions. The output format is "[MultiXactId] [count][[xid1] ([status1]), [xid2] ([status2]), ...]". The function manages its own static string buffer, freeing the previous result before creating a new one. The resulting string is allocated in TopMemoryContext to ensure it persists across memory context resets, making it suitable for error reporting and debugging scenarios.

## Parameters / Member Variables
- : The MultiXact ID to convert to string
- : The number of member transactions in the MultiXact
- : Array of MultiXactMember structures containing transaction IDs and their status

## Dependencies
- Functions called/Symbols referenced:
  - mxstatus_to_string
  - MemoryContextStrdup
  - initStringInfo
  - appendStringInfo
  - appendStringInfoChar
  - pfree
- Called from (representative examples):
  - MultiXactIdCreate
  - MultiXactIdCreateFromMembers
  - GetMultiXactIdMembers
  - mXactCacheGetBySet
  - mXactCacheGetById

## Notes and Other Information
- Uses a static buffer that is automatically freed on subsequent calls
- The returned string is allocated in TopMemoryContext for persistence
- Primarily used for debugging, logging, and error reporting
- Format: "MultiXactId count[xid1 (status1), xid2 (status2), ...]"
- Located in src/backend/access/transam/multixact.c:1769-1799