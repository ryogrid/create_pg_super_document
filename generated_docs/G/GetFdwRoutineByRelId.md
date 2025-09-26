# GetFdwRoutineByRelId

## Location
[src/backend/foreign/foreign.c:419-441](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L419-L441)

## Overview
Retrieves the FdwRoutine structure for a foreign table by first obtaining the foreign server ID and then getting the FDW routine from that server.

## Definition
```c
FdwRoutine *GetFdwRoutineByRelId(Oid relid)
```

## Detailed Description
This is a convenience function that combines the functionality of `GetForeignServerIdByRelId` and `GetFdwRoutineByServerId` into a single call. It takes a foreign table relation ID and returns the complete FdwRoutine structure containing all callback functions for the foreign data wrapper. The function serves as a wrapper that performs the two-step lookup process: first finding the foreign server associated with the table, then retrieving the FDW routine from that server.

This function is commonly used when working directly with foreign tables and needing access to their FDW functionality.

## Parameters / Member Variables
- `relid`: The OID of the foreign table relation for which to retrieve the FDW routine structure

## Dependencies
- Functions called/Symbols referenced:
  - [GetForeignServerIdByRelId](GetForeignServerIdByRelId.md)
  - [GetFdwRoutineByServerId](GetFdwRoutineByServerId.md)
- Called from (representative examples):
  - [GetFdwRoutineForRelation](GetFdwRoutineForRelation.md)
  - [make_modifytable](../m/make_modifytable.md)
  - [select_rowmark_type](../s/select_rowmark_type.md)

## Notes and Other Information
- Wrapper function that combines foreign server lookup and FDW routine retrieval
- Inherits error handling from the called functions (GetForeignServerIdByRelId and GetFdwRoutineByServerId)
- Returns a pointer to FdwRoutine structure
- More convenient than calling the individual lookup functions when starting with a relation ID
- Used in query planning and execution contexts where FDW callbacks are needed