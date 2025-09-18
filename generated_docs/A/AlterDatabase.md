# AlterDatabase

## Location
src/backend/commands/dbcommands.c: 2328 - 2500

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
  - DefElem: Definition element structure for parsing individual options
  - errorConflictingDefElem: Reports errors for duplicate options
  - PreventInTransactionBlock: Prevents certain operations within transaction blocks
  - movedb: Handles database tablespace relocation
  - defGetBoolean/defGetInt32/defGetString: Extract typed values from DefElem
  - database_is_invalid_form: Checks if database is in invalid state
  - object_ownercheck: Validates database ownership permissions
  - heap_modify_tuple: Creates modified catalog tuple
  - CatalogTupleUpdate: Updates database catalog entry
  - InvokeObjectPostAlterHook: Triggers post-alter event hooks
- Called from (representative examples):
  - standard_ProcessUtility: Main utility statement processing function

## Notes and Other Information
- Supports four main options: is_template, allow_connections, connection_limit, and tablespace
- Tablespace option cannot be combined with other options and requires special handling via movedb
- Prevents disabling connections to the currently connected database to avoid lockout
- Validates connection limits to ensure they are not below the minimum allowed value
- Uses tuple locking to prevent concurrent modifications during the update process
- Returns the database OID for most operations, InvalidOid for tablespace moves
- Includes checks for invalid databases and proper error reporting with parser position information