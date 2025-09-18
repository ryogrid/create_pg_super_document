# AlterDatabase

## Location
[src/backend/commands/dbcommands.c:2328-2500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L2328-L2500)

## Overview
AlterDatabase processes ALTER DATABASE statements to modify database properties such as template status, connection permissions, connection limits, and tablespace assignments.

## Definition
```c
Oid AlterDatabase(ParseState *pstate, AlterDatabaseStmt *stmt, bool isTopLevel)
```

## Detailed Description
AlterDatabase handles various ALTER DATABASE operations by parsing statement options and updating the corresponding database properties in the pg_database system catalog. It supports modifying the template status (is_template), connection allowance (allow_connections), connection limits (connection_limit), and tablespace assignment. For tablespace changes, it delegates to the movedb function to physically relocate database files. The function includes comprehensive validation to prevent dangerous operations like disabling connections to the current database or setting invalid connection limits.

## Parameters / Member Variables
- `pstate`: Parser state containing context information for error reporting
- `stmt`: AlterDatabaseStmt structure containing the database name and list of modification options  
- `isTopLevel`: Boolean indicating whether this is a top-level statement (affects transaction block restrictions)

## Dependencies
- Functions called/Symbols referenced:
  - AlterDatabaseStmt: Statement structure containing alter database parameters
  - [DefElem](../D/DefElem.md): Definition element structure for parsing individual options
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md): Reports errors for duplicate options
  - [PreventInTransactionBlock](../P/PreventInTransactionBlock.md): Prevents certain operations within transaction blocks
  - [movedb](../m/movedb.md): Handles database tablespace relocation
  - [defGetBoolean](../d/defGetBoolean.md)/defGetInt32/defGetString: Extract typed values from DefElem
  - [database_is_invalid_form](../d/database_is_invalid_form.md): Checks if database is in invalid state
  - [object_ownercheck](../o/object_ownercheck.md): Validates database ownership permissions
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Creates modified catalog tuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates database catalog entry
  - InvokeObjectPostAlterHook: Triggers post-alter event hooks
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md): Main utility statement processing function

## Notes and Other Information
- Supports four main options: is_template, allow_connections, connection_limit, and tablespace
- Tablespace option cannot be combined with other options and requires special handling via movedb
- Prevents disabling connections to the currently connected database to avoid lockout
- Validates connection limits to ensure they are not below the minimum allowed value
- Uses tuple locking to prevent concurrent modifications during the update process
- Returns the database OID for most operations, InvalidOid for tablespace moves
- Includes checks for invalid databases and proper error reporting with parser position information