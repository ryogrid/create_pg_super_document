# DetachPartitionFinalize

## Location
[src/backend/commands/tablecmds.c:19320-19645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L19320-L19645)

## Overview
DetachPartitionFinalize performs the final cleanup operations when detaching a partition from its parent table, handling constraint removal, foreign key management, index detachment, and catalog updates.

## Definition
```c
static void DetachPartitionFinalize(Relation rel, Relation partRel, bool concurrent, Oid defaultPartOid)
```

## Detailed Description
This function completes the partition detachment process by performing comprehensive cleanup operations:

1. **Inheritance cleanup**: Removes pg_inherits row in concurrent mode (already done in non-concurrent mode)
2. **Trigger management**: Drops cloned triggers from the partition
3. **Foreign key handling**: Detaches inherited foreign keys, updates constraint relationships, and creates necessary action triggers
4. **Index detachment**: Removes parent-child relationships between indexes and their associated constraints
5. **Catalog updates**: Updates pg_class to mark the relation as no longer a partition and clears partition bounds
6. **Identity column cleanup**: Drops identity properties from all identity columns
7. **Cache invalidation**: Ensures all relation cache entries are properly invalidated

The function is designed to be separable from the main detach operation, allowing it to be run independently if the second transaction of concurrent detachment fails.

## Parameters / Member Variables
- `rel`: The parent partitioned table relation
- `partRel`: The partition relation being detached
- `concurrent`: Boolean indicating if this is part of a concurrent detachment operation
- `defaultPartOid`: OID of the default partition (if any) for special handling

## Dependencies
- Functions called/Symbols referenced:
  - [RemoveInheritance](../R/RemoveInheritance.md)
  - [DropClonedTriggersFromPartition](DropClonedTriggersFromPartition.md)
  - [RelationGetFKeyList](../R/RelationGetFKeyList.md)
  - [ConstraintSetParentConstraint](../C/ConstraintSetParentConstraint.md)
  - [GetForeignKeyCheckTriggers](../G/GetForeignKeyCheckTriggers.md)
  - [TriggerSetParentTrigger](../T/TriggerSetParentTrigger.md)
  - [DeconstructFkConstraintRow](DeconstructFkConstraintRow.md)
  - [addFkRecurseReferenced](../a/addFkRecurseReferenced.md)
  - [GetParentedForeignKeyRefs](../G/GetParentedForeignKeyRefs.md)
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [IndexSetParentIndex](../I/IndexSetParentIndex.md)
  - [get_relation_idx_constraint_oid](../g/get_relation_idx_constraint_oid.md)
  - [ATExecDropIdentity](../A/ATExecDropIdentity.md)
  - [update_default_partition_oid](../u/update_default_partition_oid.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
- Called from (representative examples):
  - [ATExecDetachPartition](../A/ATExecDetachPartition.md)
  - [ATExecDetachPartitionFinalize](../A/ATExecDetachPartitionFinalize.md)

## Notes and Other Information
- Handles complex foreign key constraint hierarchies by distinguishing between constraints inherited from parent vs. partition-specific constraints
- Carefully manages constraint parent-child relationships to avoid orphaned constraints
- Uses extensive catalog updates to ensure consistency across pg_class, pg_constraint, and other system catalogs
- Performs recursive cache invalidation for partitioned tables to ensure all descendant partitions are properly updated
- Designed to be crash-safe and can be run independently if needed during recovery scenarios

## Simplified Source
```c
static void DetachPartitionFinalize(Relation rel, Relation partRel, bool concurrent, Oid defaultPartOid) {
    Relation classRel;
    List *foreign_keys;
    List *indexes;
    List *fkoids = NIL;
    ListCell *cell;

    // Remove inheritance if concurrent (already done in non-concurrent mode)
    if (concurrent) {
        RemoveInheritance(partRel, rel, true);
    }

    // Drop cloned triggers from partition
    DropClonedTriggersFromPartition(RelationGetRelid(partRel));

    // Handle foreign key detachment
    foreign_keys = copyObject(RelationGetFKeyList(partRel));
    if (foreign_keys != NIL) {
        Relation trigrel = table_open(TriggerRelationId, RowExclusiveLock);

        // Collect constraint OIDs to identify parent constraints
        foreach_node(ForeignKeyCacheInfo, fk, foreign_keys) {
            fkoids = lappend_oid(fkoids, fk->conoid);
        }

        // Process each foreign key constraint
        foreach(cell, foreign_keys) {
            ForeignKeyCacheInfo *fk = lfirst(cell);
            HeapTuple contup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(fk->conoid));
            Form_pg_constraint conform = (Form_pg_constraint) GETSTRUCT(contup);

            // Only process inherited foreign keys not in parent list
            if (conform->contype == CONSTRAINT_FOREIGN &&
                OidIsValid(conform->conparentid) &&
                !list_member_oid(fkoids, conform->conparentid)) {

                // Detach constraint from parent
                ConstraintSetParentConstraint(fk->conoid, InvalidOid, InvalidOid);

                // Detach check triggers
                Oid insertTriggerOid, updateTriggerOid;
                GetForeignKeyCheckTriggers(trigrel, fk->conoid, fk->confrelid, fk->conrelid,
                                         &insertTriggerOid, &updateTriggerOid);
                TriggerSetParentTrigger(trigrel, insertTriggerOid, InvalidOid, RelationGetRelid(partRel));
                TriggerSetParentTrigger(trigrel, updateTriggerOid, InvalidOid, RelationGetRelid(partRel));

                // Create action triggers on referenced table
                Constraint *fkconstraint = makeNode(Constraint);
                // Set up constraint details...
                Relation refdRel = table_open(fk->confrelid, ShareRowExclusiveLock);
                addFkRecurseReferenced(fkconstraint, partRel, refdRel, /* ... parameters ... */);
                table_close(refdRel, NoLock);
            }
            ReleaseSysCache(contup);
        }
        table_close(trigrel, RowExclusiveLock);
    }

    // Remove sub-constraints on referenced side
    foreach(cell, GetParentedForeignKeyRefs(partRel)) {
        Oid constraintOid = lfirst_oid(cell);
        ObjectAddress constraint;

        ConstraintSetParentConstraint(constraintOid, InvalidOid, InvalidOid);
        deleteDependencyRecordsForClass(ConstraintRelationId, constraintOid,
                                       ConstraintRelationId, DEPENDENCY_INTERNAL);
        CommandCounterIncrement();

        ObjectAddressSet(constraint, ConstraintRelationId, constraintOid);
        performDeletion(&constraint, DROP_RESTRICT, 0);
    }

    // Detach indexes
    indexes = RelationGetIndexList(partRel);
    foreach(cell, indexes) {
        Oid idxid = lfirst_oid(cell);
        if (has_superclass(idxid)) {
            Oid parentidx = get_partition_parent(idxid, false);
            Relation idx = index_open(idxid, AccessExclusiveLock);

            IndexSetParentIndex(idx, InvalidOid);

            // Detach associated constraints if they exist
            Oid constraintOid = get_relation_idx_constraint_oid(RelationGetRelid(partRel), idxid);
            Oid parentConstraintOid = get_relation_idx_constraint_oid(RelationGetRelid(rel), parentidx);
            if (OidIsValid(parentConstraintOid) && OidIsValid(constraintOid)) {
                ConstraintSetParentConstraint(constraintOid, InvalidOid, InvalidOid);
            }

            index_close(idx, NoLock);
        }
    }

    // Update pg_class: clear partition bound and set relispartition = false
    classRel = table_open(RelationRelationId, RowExclusiveLock);
    HeapTuple tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(partRel)));

    Datum new_val[Natts_pg_class];
    bool new_null[Natts_pg_class], new_repl[Natts_pg_class];
    memset(new_val, 0, sizeof(new_val));
    memset(new_null, false, sizeof(new_null));
    memset(new_repl, false, sizeof(new_repl));

    new_val[Anum_pg_class_relpartbound - 1] = (Datum) 0;
    new_null[Anum_pg_class_relpartbound - 1] = true;
    new_repl[Anum_pg_class_relpartbound - 1] = true;

    HeapTuple newtuple = heap_modify_tuple(tuple, RelationGetDescr(classRel),
                                          new_val, new_null, new_repl);
    ((Form_pg_class) GETSTRUCT(newtuple))->relispartition = false;
    CatalogTupleUpdate(classRel, &newtuple->t_self, newtuple);

    heap_freetuple(newtuple);
    table_close(classRel, RowExclusiveLock);

    // Drop identity properties from all identity columns
    for (int attno = 0; attno < RelationGetNumberOfAttributes(partRel); attno++) {
        Form_pg_attribute attr = TupleDescAttr(partRel->rd_att, attno);
        if (!attr->attisdropped && attr->attidentity) {
            ATExecDropIdentity(partRel, NameStr(attr->attname), false,
                             AccessExclusiveLock, true, true);
        }
    }

    // Handle default partition updates
    if (OidIsValid(defaultPartOid)) {
        if (RelationGetRelid(partRel) == defaultPartOid) {
            update_default_partition_oid(RelationGetRelid(rel), InvalidOid);
        } else {
            CacheInvalidateRelcacheByRelid(defaultPartOid);
        }
    }

    // Invalidate relation caches
    CacheInvalidateRelcache(rel);
    if (partRel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
        List *children = find_all_inheritors(RelationGetRelid(partRel), AccessExclusiveLock, NULL);
        foreach(cell, children) {
            CacheInvalidateRelcacheByRelid(lfirst_oid(cell));
        }
    }
}
```