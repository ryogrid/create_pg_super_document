# ATExecSetTableSpaceNoStorage

## Location
src/backend/commands/tablecmds.c: 15346 - 15384

## Overview
ATExecSetTableSpaceNoStorage handles ALTER TABLE SET TABLESPACE operations for relations that have no physical storage, performing metadata-only updates to change the tablespace assignment.

## Definition
```c
static void ATExecSetTableSpaceNoStorage(Relation rel, Oid newTableSpace)
```

## Detailed Description
This function provides specialized handling for relations that do not have physical storage but still maintain tablespace associations for logical purposes. Unlike relations with storage that require data copying, these relations can have their tablespace updated through a simple metadata operation that only modifies the pg_class catalog entry.

The function validates that the relation indeed has no storage using the RELKIND_HAS_STORAGE macro, checks if the tablespace move is permitted, and then updates the relation's tablespace assignment through SetRelationTableSpace. This approach is much more efficient than the full data copying process used for relations with storage, as it only involves catalog updates without any physical data movement.

## Parameters / Member Variables
- `rel`: The relation being moved to a new tablespace
- `newTableSpace`: Object identifier of the destination tablespace

## Dependencies
- Functions called/Symbols referenced:
  - RELKIND_HAS_STORAGE: Macro to check if relation kind has physical storage
  - [CheckRelationTableSpaceMove](../C/CheckRelationTableSpaceMove.md): Validates whether the tablespace move is allowed
  - [SetRelationTableSpace](../S/SetRelationTableSpace.md): Updates the pg_class catalog with new tablespace information
  - InvokeObjectPostAlterHook: Triggers post-alter hooks for dependency tracking
  - CommandCounterIncrement: Makes catalog changes visible to subsequent operations

- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md): Main ALTER TABLE command execution dispatcher

## Notes and Other Information
- Only processes relations without storage (e.g., views, foreign tables, partitioned tables)
- Uses an assertion to ensure it's not called on relations with storage
- Much more efficient than ATExecSetTableSpace as it avoids data copying
- Updates only metadata in the system catalog, not physical storage
- Maintains consistency with the ALTER TABLE command framework through hook invocations
- Makes changes immediately visible through CommandCounterIncrement for subsequent operations
- Part of the broader ALTER TABLE execution framework that routes different relation types to appropriate handlers