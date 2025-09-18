# GetFdwRoutine

## Location
src/backend/foreign/foreign.c: 325 - 354

## Overview
Calls a foreign data wrapper handler function to retrieve its FdwRoutine structure containing the FDW's callback functions.

## Definition
```c
FdwRoutine *GetFdwRoutine(Oid fdwhandler)
```

## Detailed Description
GetFdwRoutine is a critical function that interfaces with foreign data wrapper handler functions to obtain their FdwRoutine structures. The FdwRoutine contains function pointers to all the callback functions that the FDW implements (such as GetForeignRelSize, GetForeignPaths, GetForeignPlan, etc.). The function includes security checks to ensure that access to foreign tables is not restricted by system policies. It validates that the handler function returns a proper FdwRoutine structure and raises an error if the returned value is invalid or NULL.

## Parameters / Member Variables
- `fdwhandler`: Object ID of the foreign data wrapper handler function to call

## Dependencies
- Functions called/Symbols referenced:
  - OidFunctionCall0
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - IsA (macro)
  - elog
  - ereport/errmsg/errcode
  - RESTRICT_RELKIND_FOREIGN_TABLE
- Called from (representative examples):
  - [ImportForeignSchema](../I/ImportForeignSchema.md)
  - [GetFdwRoutineByServerId](GetFdwRoutineByServerId.md)

## Notes and Other Information
The function performs a security check against restrict_nonsystem_relation_kind to prevent access to foreign tables when such access is administratively restricted. The returned FdwRoutine structure is the primary interface between PostgreSQL's query planner/executor and the foreign data wrapper implementation. Each FDW must implement a handler function that returns a properly initialized FdwRoutine. The function is located in src/backend/foreign/foreign.c:325-354 and is fundamental to the FDW architecture.