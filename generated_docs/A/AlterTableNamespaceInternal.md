# AlterTableNamespaceInternal

## Location
[src/backend/commands/tablecmds.c:17278-17314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L17278-L17314)

## Overview
AlterTableNamespaceInternal performs the core work of moving a table or materialized view to another namespace by relocating the relation itself and all its dependent objects to the new schema.

## Definition

```c
void
AlterTableNamespaceInternal(Relation rel, Oid oldNspOid, Oid nspOid,
							ObjectAddresses *objsMoved)
```
## Detailed Description
This function implements the low-level mechanics of namespace relocation for tables and materialized views. It systematically moves all components and dependencies of a relation to the target schema:

1. Updates the pg_class row and pg_depend entries for the main relation
2. Relocates the table's row type (composite type) if it exists
3. Moves all associated indexes to the new namespace
4. Relocates owned sequences to the new namespace  
5. Updates constraint namespaces for all table constraints

The function ensures atomicity by tracking all moved objects in the objsMoved parameter, allowing for proper rollback if needed. It uses RowExclusiveLock on the pg_class catalog to ensure consistency during the relocation process.

## Parameters / Member Variables
- `rel`: The Relation structure representing the table or materialized view being moved
- `oldNspOid`: The OID of the source namespace (current schema)
- `nspOid`: The OID of the target namespace (destination schema)
- `*objsMoved`: ObjectAddresses structure to track all objects moved during the operation
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [AlterRelationNamespaceInternal](AlterRelationNamespaceInternal.md)
  - [AlterTypeNamespaceInternal](AlterTypeNamespaceInternal.md)
  - [AlterIndexNamespaces](AlterIndexNamespaces.md)
  - [AlterSeqNamespaces](AlterSeqNamespaces.md)
  - [AlterConstraintNamespaces](AlterConstraintNamespaces.md)
  - [table_close](../t/table_close.md)

- Called from (representative examples):
  - [AlterTableNamespace](AlterTableNamespace.md) (high-level ALTER TABLE SET SCHEMA handler)
  - [AlterObjectNamespace_oid](AlterObjectNamespace_oid.md) (generic object namespace alteration)

## Notes and Other Information
- Requires objsMoved parameter to be non-NULL for proper object tracking
- Uses RowExclusiveLock on RelationRelationId (pg_class) during the operation
- Handles composite types associated with tables by moving them to the same namespace
- Systematically processes all dependent object types: indexes, sequences, constraints
- The function is the "guts" of table namespace relocation as noted in the comment
- Maintains referential integrity by ensuring all dependent objects move together
- Uses AccessExclusiveLock when moving sequences to prevent concurrent access issues
- Part of a coordinated effort where the high-level function handles validation and this function performs the actual work

## Simplified Source

```c
void AlterTableNamespaceInternal(Relation rel, Oid oldNspOid, Oid nspOid, ObjectAddresses *objsMoved)
{
    Relation classRel;

    Assert(objsMoved != NULL);

    // Open pg_class catalog with row exclusive lock
    classRel = table_open(RelationRelationId, RowExclusiveLock);

    // Move the main relation to new namespace
    AlterRelationNamespaceInternal(classRel, RelationGetRelid(rel), oldNspOid, nspOid, true, objsMoved);

    // Move the table's row type if it exists
    if (OidIsValid(rel->rd_rel->reltype))
        AlterTypeNamespaceInternal(rel->rd_rel->reltype, nspOid, false, false, false, objsMoved);

    // Move all dependent objects to new namespace
    AlterIndexNamespaces(classRel, rel, oldNspOid, nspOid, objsMoved);
    AlterSeqNamespaces(classRel, rel, oldNspOid, nspOid, objsMoved, AccessExclusiveLock);
    AlterConstraintNamespaces(RelationGetRelid(rel), oldNspOid, nspOid, false, objsMoved);

    table_close(classRel, RowExclusiveLock);
}
```