# DropTableSpaceStmt

## Location
[src/include/nodes/parsenodes.h:2789-2794](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2789-L2794)

## Overview
DropTableSpaceStmt is a parse tree node structure that represents a DROP TABLESPACE statement, used to remove an existing tablespace from the PostgreSQL database system.

## Definition
```c
typedef struct DropTableSpaceStmt
{
    NodeTag     type;
    char       *tablespacename;
    bool        missing_ok;     /* skip error if missing? */
} DropTableSpaceStmt;
```

## Detailed Description
DropTableSpaceStmt represents the parsed form of a DROP TABLESPACE command in PostgreSQL. This statement removes a tablespace from the database system, including its catalog entries and associated filesystem directory structures.

The structure is simpler than its CREATE counterpart, requiring only the tablespace name to identify what to drop and a flag to control error handling when the tablespace does not exist. The missing_ok flag implements the IF EXISTS clause functionality, allowing the statement to succeed silently if the specified tablespace is not found.

Before a tablespace can be dropped, it must be empty (containing no database objects) and not be the default tablespace for any database. The drop operation removes the tablespace entry from the system catalogs and may also remove the associated directory structure from the filesystem.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a DropTableSpaceStmt node type
- `tablespacename`: String containing the name of the tablespace to drop
- `missing_ok`: Boolean flag to skip errors if the tablespace does not exist (implements IF EXISTS clause)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)

- Called from (representative examples):
  - DropTableSpace (src/backend/commands/tablespace.c:395)
  - standard_ProcessUtility (src/backend/tcop/utility.c:719)

## Notes and Other Information
- Tablespace removal requires superuser privileges due to filesystem access requirements
- The tablespace must be empty before it can be dropped - no database objects can be stored in it
- The tablespace cannot be the default tablespace for any database when dropped
- When missing_ok is true, the statement implements DROP TABLESPACE IF EXISTS behavior
- The operation removes both the catalog entries and potentially the filesystem directory
- System tablespaces (pg_default, pg_global) cannot be dropped
- Active connections using the tablespace may prevent successful deletion
- The drop operation is atomic - either the tablespace is completely removed or the operation fails