# RememberConstraintForRebuilding

## Location
[src/backend/commands/tablecmds.c:13720-13759](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L13720-L13759)

## Overview
RememberConstraintForRebuilding records constraint information that needs to be preserved and recreated when a constraint requires rebuilding during table alterations.

## Definition

```c
static void
RememberConstraintForRebuilding(Oid conoid, AlteredTableInfo *tab)
```
## Detailed Description
This function is responsible for capturing constraint information before it gets modified or dropped during table alterations. It performs several critical tasks:

1. **Deduplication Check**: Verifies the constraint hasn't already been recorded to prevent duplicate rebuilding attempts
2. **Definition Capture**: Uses pg_get_constraintdef_command() to capture the constraint's current definition string before any modifications occur
3. **List Management**: Adds the constraint OID and definition to the AlteredTableInfo tracking lists
4. **Index Property Preservation**: For constraints that have associated indexes, it calls helper functions to preserve:
   - Replica identity status (via RememberReplicaIdentityForRebuilding)
   - Clustered index status (via RememberClusterOnForRebuilding)

The deduplication check is critical because:
- It prevents attempting to recreate the same constraint multiple times
- When a constraint depends on multiple columns being altered, the definition must be captured before any column changes (ruleutils.c would get confused if asked to generate the definition after partial changes)

## Parameters / Member Variables
- : OID of the constraint to be remembered for rebuilding
- : AlteredTableInfo structure containing lists of constraints to rebuild

## Dependencies
- Functions called/Symbols referenced:
  - [list_member_oid](../l/list_member_oid.md)
  - [pg_get_constraintdef_command](../p/pg_get_constraintdef_command.md)
  - lappend_oid
  - lappend
  - [get_constraint_index](../g/get_constraint_index.md)
  - [RememberReplicaIdentityForRebuilding](RememberReplicaIdentityForRebuilding.md)
  - [RememberClusterOnForRebuilding](RememberClusterOnForRebuilding.md)
- Called from (representative examples):
  - [RememberAllDependentForRebuilding](RememberAllDependentForRebuilding.md)
  - [RememberIndexForRebuilding](RememberIndexForRebuilding.md)

## Notes and Other Information
- Essential for maintaining constraint integrity during table schema changes
- The captured definition string will be used later to recreate the constraint with identical semantics
- Handles both the constraint itself and any special properties of its associated index
- Part of the broader constraint rebuilding infrastructure in PostgreSQL's ALTER TABLE implementation