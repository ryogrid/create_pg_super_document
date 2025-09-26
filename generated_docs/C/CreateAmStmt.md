# CreateAmStmt

## Location
[src/include/nodes/parsenodes.h:2989-2995](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2989-L2995)

## Overview
CreateAmStmt represents the parsed structure of a CREATE ACCESS METHOD SQL statement, used to register new access methods in PostgreSQL.

## Definition
```c
typedef struct CreateAmStmt
{
	NodeTag		type;
	char	   *amname;			/* access method name */
	List	   *handler_name;	/* handler function name */
	char		amtype;			/* type of access method */
} CreateAmStmt;
```

## Detailed Description
CreateAmStmt is a parse tree node that captures the components of a CREATE ACCESS METHOD statement. Access methods in PostgreSQL define how data is stored and retrieved, with two main types: index access methods (for creating indexes) and table access methods (for storing table data). The statement registers a new access method by specifying its name, the handler function that implements its operations, and the type of access method (index or table).

## Parameters / Member Variables
- `type`: NodeTag identifying this as a CreateAmStmt node
- `amname`: The name of the new access method to be created
- `handler_name`: List containing the qualified name of the handler function that implements the access method interface
- `amtype`: Single character indicating the access method type (AMTYPE_INDEX 'i' or AMTYPE_TABLE 't')

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating the node)
  - AMTYPE_INDEX, AMTYPE_TABLE (access method type constants)
  - NodeTag (for type identification)
- Called from (representative examples):
  - CreateAccessMethod (in src/backend/commands/amcmds.c:43)
  - ProcessUtilitySlow (in src/backend/tcop/utility.c:1839)

## Notes and Other Information
- Part of PostgreSQL's extensible access method system
- Parsed in gram.y rule CreateAmStmt (line 5877) with syntax 'CREATE ACCESS METHOD name TYPE {INDEX|TABLE} HANDLER handler_name'
- Requires superuser privileges to execute
- The handler function must conform to the appropriate access method interface (IndexAmRoutine for index AMs, TableAmRoutine for table AMs)
- Processed by the CreateAccessMethod function in src/backend/commands/amcmds.c
- Related to T_CreateAmStmt case in utility command processing