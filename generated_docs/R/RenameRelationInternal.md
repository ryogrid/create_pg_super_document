# RenameRelationInternal

## Location
[src/backend/commands/tablecmds.c:4135-4227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L4135-L4227)

## Overview
RenameRelationInternal is the core internal function that performs the actual relation renaming operation, handling all necessary catalog updates and associated object renaming.

## Definition
void RenameRelationInternal(Oid myrelid, const char *newrelname, bool is_internal, bool is_index)

## Detailed Description
RenameRelationInternal implements the low-level mechanics of renaming a database relation by directly updating the pg_class catalog. The function acquires appropriate locks (ShareUpdateExclusiveLock for indexes, AccessExclusiveLock for other relations), checks for name conflicts, and updates the relation name in the system catalog.

Beyond the basic renaming, the function handles associated object renaming including the relation type (for composite types) and constraint names (for indexes with associated constraints). It ensures consistency by maintaining locks until transaction end and invoking post-alter hooks for proper event notification.

## Parameters / Member Variables
- `myrelid`: OID of the relation to rename
- `newrelname`: New name for the relation
- `is_internal`: Whether this is an internal operation (affects hook invocation)
- `is_index`: Whether the relation is an index (affects lock level)

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md)
  - RelationGetNamespace
  - [table_open](../t/table_open.md)/table_close
  - [SearchSysCacheLockedCopy1](../S/SearchSysCacheLockedCopy1.md)
  - [get_relname_relid](../g/get_relname_relid.md)
  - [namestrcpy](../n/namestrcpy.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [UnlockTuple](../U/UnlockTuple.md)
  - InvokeObjectPostAlterHookArg
  - [heap_freetuple](../h/heap_freetuple.md)
  - [RenameTypeInternal](RenameTypeInternal.md)
  - [get_index_constraint](../g/get_index_constraint.md)
  - [RenameConstraintById](RenameConstraintById.md)
  - [relation_close](../r/relation_close.md)
- Called from (representative examples):
  - [RenameRelation](RenameRelation.md) (in src/backend/commands/tablecmds.c)
  - [rename_constraint_internal](../r/rename_constraint_internal.md) (in src/backend/commands/tablecmds.c)
  - [finish_heap_swap](../f/finish_heap_swap.md) (in src/backend/commands/cluster.c)
  - [RenameType](RenameType.md) (in src/backend/commands/typecmds.c)

## Notes and Other Information
- Uses different lock levels based on object type (ShareUpdateExclusiveLock for indexes, AccessExclusiveLock for others)
- Maintains exclusive locks until transaction end to prevent concurrent modifications
- Automatically renames associated composite types when they exist
- Renames associated constraints for indexes that have constraints
- Performs duplicate name checking before proceeding with the rename
- Handles both user-initiated and internal rename operations
- Updates system catalogs directly using low-level catalog functions
- Invokes post-alter hooks for proper event trigger and extension support

## Simplified Source

```c
void RenameRelationInternal(Oid myrelid, const char *newrelname, bool is_internal, bool is_index) {
    Relation targetrelation;
    Relation relrelation;
    ItemPointerData otid;
    HeapTuple reltup;
    Form_pg_class relform;
    Oid namespaceId;

    // Open target relation with appropriate lock level
    targetrelation = relation_open(myrelid, is_index ? ShareUpdateExclusiveLock : AccessExclusiveLock);
    namespaceId = RelationGetNamespace(targetrelation);

    // Open pg_class catalog for update
    relrelation = table_open(RelationRelationId, RowExclusiveLock);

    // Get relation tuple and check for name conflicts
    reltup = SearchSysCacheLockedCopy1(RELOID, ObjectIdGetDatum(myrelid));
    if (!HeapTupleIsValid(reltup))
        elog(ERROR, "cache lookup failed for relation %u", myrelid);

    otid = reltup->t_self;
    relform = (Form_pg_class) GETSTRUCT(reltup);

    // Check if new name already exists
    if (get_relname_relid(newrelname, namespaceId) != InvalidOid)
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_TABLE),
                       errmsg("relation \"%s\" already exists", newrelname)));

    // Validate index handling consistency
    Assert(!is_index || is_index == (targetrelation->rd_rel->relkind == RELKIND_INDEX ||
                                    targetrelation->rd_rel->relkind == RELKIND_PARTITIONED_INDEX));

    // Update relation name in pg_class
    namestrcpy(&(relform->relname), newrelname);
    CatalogTupleUpdate(relrelation, &otid, reltup);
    UnlockTuple(relrelation, &otid, InplaceUpdateTupleLock);

    // Invoke post-alter hook
    InvokeObjectPostAlterHookArg(RelationRelationId, myrelid, 0, InvalidOid, is_internal);

    // Clean up catalog access
    heap_freetuple(reltup);
    table_close(relrelation, RowExclusiveLock);

    // Rename associated type if it exists
    if (OidIsValid(targetrelation->rd_rel->reltype))
        RenameTypeInternal(targetrelation->rd_rel->reltype, newrelname, namespaceId);

    // Rename associated constraint for indexes
    if (targetrelation->rd_rel->relkind == RELKIND_INDEX ||
        targetrelation->rd_rel->relkind == RELKIND_PARTITIONED_INDEX) {
        Oid constraintId = get_index_constraint(myrelid);
        if (OidIsValid(constraintId))
            RenameConstraintById(constraintId, newrelname);
    }

    // Close relation but keep lock
    relation_close(targetrelation, NoLock);
}
```