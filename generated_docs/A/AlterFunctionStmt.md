# AlterFunctionStmt

## Location
[src/include/nodes/parsenodes.h:3460-3466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3460-L3466)

## Overview
AlterFunctionStmt is a node structure representing an SQL ALTER FUNCTION statement in PostgreSQL's parse tree. It encapsulates the information needed to modify the properties of an existing function or procedure.

## Definition

```c
typedef struct AlterFunctionStmt
{
	NodeTag		type;
	ObjectType	objtype;
	ObjectWithArgs *func;		/* name and args of function */
	List	   *actions;		/* list of DefElem */
} AlterFunctionStmt;
```
## Detailed Description
This structure is used during the parsing phase to represent ALTER FUNCTION and ALTER PROCEDURE statements. It stores the target function/procedure identification along with the list of alterations to be applied. The structure is part of PostgreSQL's node system for representing parsed SQL statements and is processed during the utility command execution phase.

## Parameters / Member Variables
- : NodeTag identifying this as an AlterFunctionStmt node
- : ObjectType specifying whether this targets a function or procedure 
- : Pointer to ObjectWithArgs containing the function name and parameter signature for identification
- : List of DefElem structures representing the alterations to apply (e.g., VOLATILE, IMMUTABLE, RENAME TO, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - ObjectType
  - ObjectWithArgs
- Called from (representative examples):
  - AlterFunction (src/backend/commands/functioncmds.c:1343)
  - ProcessUtilitySlow (src/backend/tcop/utility.c:1659)
  - CreateCommandTag (src/backend/tcop/utility.c:2707)

## Notes and Other Information
This structure is created during SQL parsing and consumed during command execution. The actual function modification logic is handled by the AlterFunction function in functioncmds.c, which processes the actions list to apply the requested changes to the function's catalog entries.