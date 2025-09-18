# ExecRenameStmt

## Location
src/backend/commands/alter.c: 357 - 456

## Overview
A dispatcher function that executes ALTER OBJECT RENAME TO statements by routing to the appropriate object-type-specific rename function based on the rename statement type.

## Definition
```c
ObjectAddress ExecRenameStmt(RenameStmt *stmt)
```

## Detailed Description
This function serves as the main entry point for processing ALTER ... RENAME TO SQL statements in PostgreSQL. It examines the renameType field of the RenameStmt structure and dispatches to the appropriate object-type-specific rename function. For simple objects that can be renamed through generic catalog table updates, it uses the AlterObjectRename_internal function with proper locking. For more complex objects like tables, constraints, and triggers, it calls specialized rename functions that handle the additional complexity. The function returns an ObjectAddress representing the renamed object.

## Parameters / Member Variables
- `stmt`: Pointer to RenameStmt structure containing the rename operation details including object type, old name, new name, and other context

## Dependencies
- Functions called/Symbols referenced:
  - Object-specific rename functions:
    - RenameConstraint (for table and domain constraints)
    - RenameDatabase (for databases)
    - RenameRole (for roles)
    - RenameSchema (for schemas)
    - RenameTableSpace (for tablespaces)
    - RenameRelation (for tables, sequences, views, materialized views, indexes, foreign tables)
    - renameatt (for columns/attributes)
    - RenameRewriteRule (for rules)
    - renametrig (for triggers)
    - rename_policy (for policies)
    - RenameType (for domains and types)
  - Generic rename infrastructure:
    - get_object_address (to resolve object identity)
    - table_open/table_close (for catalog access)
    - AlterObjectRename_internal (for simple objects)
  - Object type constants (OBJECT_*)
  - AccessExclusiveLock, RowExclusiveLock (locking modes)

- Called from (representative examples):
  - standard_ProcessUtility (src/backend/tcop/utility.c:996)
  - ProcessUtilitySlow (src/backend/tcop/utility.c:1777)

## Notes and Other Information
- Public function (not static), part of the command execution interface
- Declared in src/include/commands/alter.h:21
- Handles a comprehensive set of PostgreSQL object types through a large switch statement
- Uses appropriate locking strategies: AccessExclusiveLock for object resolution, RowExclusiveLock for catalog updates
- For simple objects (aggregates, collations, functions, etc.), follows a common pattern: resolve object address, open catalog, call AlterObjectRename_internal, close catalog
- Returns ObjectAddress to provide information about the renamed object to the caller
- Part of PostgreSQL's utility command processing pipeline
- Designed to be extensible - new object types can be added by extending the switch statement