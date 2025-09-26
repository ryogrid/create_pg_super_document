# AlterTableLookupRelation

## Location
[src/backend/commands/tablecmds.c:4340-4398](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L4340-L4398)

## Overview
Looks up and locks the OID for a relation specified in an ALTER TABLE statement, handling missing relation scenarios and providing appropriate callback processing.

## Definition
```c
Oid AlterTableLookupRelation(AlterTableStmt *stmt, LOCKMODE lockmode)
```

## Detailed Description
This function serves as a specialized wrapper around the general relation lookup mechanism specifically for ALTER TABLE operations. It resolves a relation name to its OID while simultaneously acquiring the specified lock on the relation. The function integrates several important features:

1. **Relation Resolution**: Converts the relation name from the ALTER TABLE statement into a concrete OID that can be used for subsequent operations.

2. **Lock Acquisition**: Acquires the specified lock mode on the relation to ensure proper concurrency control during the ALTER TABLE operation.

3. **Missing Relation Handling**: Respects the `missing_ok` flag from the ALTER TABLE statement to either allow or prevent operations on non-existent relations (supporting IF EXISTS syntax).

4. **Callback Integration**: Uses a specialized callback function (RangeVarCallbackForAlterRelation) that can perform additional validation or processing specific to ALTER TABLE operations during the lookup process.

The function essentially bridges the gap between the parsed ALTER TABLE statement and the low-level relation manipulation functions by providing a standardized way to locate and lock the target relation.

## Parameters / Member Variables
- `stmt`: Pointer to the parsed ALTER TABLE statement containing the relation name and options
- `lockmode`: The type of lock to acquire on the relation (e.g., AccessExclusiveLock for DDL operations)

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md) (general relation lookup function with extended options)
  - [RangeVarCallbackForAlterRelation](../R/RangeVarCallbackForAlterRelation.md) (specialized callback for ALTER TABLE relation validation)
  - [AlterTableStmt](AlterTableStmt.md) (structure type for parsed ALTER TABLE statements)
  - RVR_MISSING_OK (flag constant for handling missing relations)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (in utility.c:1300, during utility command processing for ALTER TABLE statements)

## Notes and Other Information
- This function is part of the ALTER TABLE command processing pipeline, specifically handling the initial relation lookup phase
- The function respects SQL standard IF EXISTS semantics through the missing_ok flag handling
- Uses RangeVarGetRelidExtended rather than simpler lookup functions to provide full callback support and extended options
- The callback mechanism allows for additional security checks, permission validation, or other ALTER TABLE-specific processing during lookup
- Returns the OID of the found relation, which becomes the primary identifier for subsequent ALTER TABLE processing steps
- The lock acquisition at lookup time is critical for ensuring that the relation structure doesn't change between lookup and the actual ALTER operations
- Part of PostgreSQL's general pattern of early lock acquisition to prevent race conditions in DDL operations