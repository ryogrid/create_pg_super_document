# index_concurrently_swap

## Location
[src/backend/catalog/index.c:1549-1819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L1549-L1819)

## Overview
index_concurrently_swap swaps the identity, dependencies, and constraints between a new concurrent index and the old index it's replacing, effectively completing the concurrent index replacement.

## Definition

```c
void
index_concurrently_swap(Oid newIndexId, Oid oldIndexId, const char *oldName)
```
## Detailed Description
This function performs the final phase of concurrent index operations by swapping all metadata between the new and old indexes. It swaps names in pg_class, transfers all constraint flags and validity states in pg_index, moves all associated constraints and triggers to point to the new index, transfers comments, handles partition inheritance relationships, swaps all dependencies, and copies statistics.

The operation is comprehensive, ensuring that the new index takes over the complete identity of the old index while the old index is marked as invalid and ready for cleanup. This includes moving primary key, exclusion, and uniqueness constraints, updating trigger references, and maintaining proper dependency relationships throughout the system.

## Parameters / Member Variables
- `newIndexId`: Object identifier of the new index that will replace the old one
- `oldIndexId`: Object identifier of the old index being replaced
- `*oldName`: Name to assign to the old index after the swap
## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md) (to lock both indexes)
  - SearchSysCacheCopy1 (for catalog tuple retrieval)
  - [namestrcpy](../n/namestrcpy.md) (for name swapping)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (for catalog updates)
  - [heap_freetuple](../h/heap_freetuple.md) (for memory cleanup)
  - [get_index_ref_constraints](../g/get_index_ref_constraints.md)/get_index_constraint (for constraint lookup)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext (for trigger scanning)
  - [heap_copytuple](../h/heap_copytuple.md)/heap_modify_tuple (for tuple manipulation)
  - [get_rel_relispartition](../g/get_rel_relispartition.md)/get_partition_ancestors (for partition handling)
  - [DeleteInheritsTuple](../D/DeleteInheritsTuple.md)/StoreSingleInheritance (for inheritance updates)
  - [changeDependenciesOf](../c/changeDependenciesOf.md)/changeDependenciesOn (for dependency swapping)
  - [pgstat_copy_relation_stats](../p/pgstat_copy_relation_stats.md) (for statistics transfer)
  - [CopyStatistics](../C/CopyStatistics.md) (for pg_statistic data transfer)
  - [relation_close](../r/relation_close.md) (for cleanup)
- Called from (representative examples):
  - Concurrent reindex completion operations

## Notes and Other Information
- This is a void function that performs extensive catalog modifications
- Uses ShareUpdateExclusiveLock on both indexes to prevent concurrent modifications
- Swaps names, constraint flags, validity states, and partition flags between indexes
- Moves all constraints (primary key, unique, exclusion) to the new index
- Updates trigger constraint references to point to the new index
- Transfers comments from old to new index via pg_description updates
- Handles partition inheritance by updating pg_inherits relationships
- Performs complete dependency swapping to maintain referential integrity
- Copies relation statistics and pg_statistic data to the new index
- Marks the old index as invalid while making the new index valid and ready
- Does not call CommandCounterIncrement() to avoid duplicate pg_depend entries
- Maintains locks until transaction end but closes relations immediately
- Located at src/backend/catalog/index.c:1549-1819

## Simplified Source

```c
void index_concurrently_swap(Oid newIndexId, Oid oldIndexId, const char *oldName) {
    Relation pg_class, pg_index, pg_constraint, pg_trigger;
    Relation oldClassRel, newClassRel;
    HeapTuple oldClassTuple, newClassTuple;
    HeapTuple oldIndexTuple, newIndexTuple;
    bool isPartition;
    Oid indexConstraintOid;
    List *constraintOids = NIL;

    // Lock both indexes
    oldClassRel = relation_open(oldIndexId, ShareUpdateExclusiveLock);
    newClassRel = relation_open(newIndexId, ShareUpdateExclusiveLock);

    // Swap names in pg_class
    pg_class = table_open(RelationRelationId, RowExclusiveLock);

    oldClassTuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(oldIndexId));
    newClassTuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(newIndexId));

    if (!HeapTupleIsValid(oldClassTuple) || !HeapTupleIsValid(newClassTuple)) {
        elog(ERROR, "could not find tuple for relations");
    }

    Form_pg_class oldClassForm = (Form_pg_class) GETSTRUCT(oldClassTuple);
    Form_pg_class newClassForm = (Form_pg_class) GETSTRUCT(newClassTuple);

    // Swap names and partition flags
    namestrcpy(&newClassForm->relname, NameStr(oldClassForm->relname));
    namestrcpy(&oldClassForm->relname, oldName);

    isPartition = newClassForm->relispartition;
    newClassForm->relispartition = oldClassForm->relispartition;
    oldClassForm->relispartition = isPartition;

    CatalogTupleUpdate(pg_class, &oldClassTuple->t_self, oldClassTuple);
    CatalogTupleUpdate(pg_class, &newClassTuple->t_self, newClassTuple);

    heap_freetuple(oldClassTuple);
    heap_freetuple(newClassTuple);

    // Swap index metadata in pg_index
    pg_index = table_open(IndexRelationId, RowExclusiveLock);

    oldIndexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(oldIndexId));
    newIndexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(newIndexId));

    Form_pg_index oldIndexForm = (Form_pg_index) GETSTRUCT(oldIndexTuple);
    Form_pg_index newIndexForm = (Form_pg_index) GETSTRUCT(newIndexTuple);

    // Transfer constraint flags from old to new index
    newIndexForm->indisprimary = oldIndexForm->indisprimary;
    oldIndexForm->indisprimary = false;
    newIndexForm->indisexclusion = oldIndexForm->indisexclusion;
    oldIndexForm->indisexclusion = false;
    newIndexForm->indimmediate = oldIndexForm->indimmediate;
    oldIndexForm->indimmediate = true;

    // Preserve other important flags
    newIndexForm->indisreplident = oldIndexForm->indisreplident;
    newIndexForm->indisclustered = oldIndexForm->indisclustered;

    // Mark new index valid, old index invalid
    newIndexForm->indisvalid = true;
    oldIndexForm->indisvalid = false;
    oldIndexForm->indisclustered = false;
    oldIndexForm->indisreplident = false;

    CatalogTupleUpdate(pg_index, &oldIndexTuple->t_self, oldIndexTuple);
    CatalogTupleUpdate(pg_index, &newIndexTuple->t_self, newIndexTuple);

    heap_freetuple(oldIndexTuple);
    heap_freetuple(newIndexTuple);

    // Move constraints and triggers to new index
    constraintOids = get_index_ref_constraints(oldIndexId);
    indexConstraintOid = get_index_constraint(oldIndexId);

    if (OidIsValid(indexConstraintOid)) {
        constraintOids = lappend_oid(constraintOids, indexConstraintOid);
    }

    pg_constraint = table_open(ConstraintRelationId, RowExclusiveLock);
    pg_trigger = table_open(TriggerRelationId, RowExclusiveLock);

    // Update all constraints to point to new index
    foreach_oid(constraintOid, constraintOids) {
        HeapTuple constraintTuple = SearchSysCacheCopy1(CONSTROID,
                                                       ObjectIdGetDatum(constraintOid));
        if (HeapTupleIsValid(constraintTuple)) {
            Form_pg_constraint conForm = (Form_pg_constraint) GETSTRUCT(constraintTuple);
            if (conForm->conindid == oldIndexId) {
                conForm->conindid = newIndexId;
                CatalogTupleUpdate(pg_constraint, &constraintTuple->t_self, constraintTuple);
            }
            heap_freetuple(constraintTuple);
        }

        // Update trigger constraint references
        ScanKeyData key[1];
        SysScanDesc scan;
        ScanKeyInit(&key[0], Anum_pg_trigger_tgconstraint,
                    BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(constraintOid));

        scan = systable_beginscan(pg_trigger, TriggerConstraintIndexId, true, NULL, 1, key);

        HeapTuple triggerTuple;
        while (HeapTupleIsValid((triggerTuple = systable_getnext(scan)))) {
            Form_pg_trigger tgForm = (Form_pg_trigger) GETSTRUCT(triggerTuple);
            if (tgForm->tgconstrindid == oldIndexId) {
                triggerTuple = heap_copytuple(triggerTuple);
                tgForm = (Form_pg_trigger) GETSTRUCT(triggerTuple);
                tgForm->tgconstrindid = newIndexId;
                CatalogTupleUpdate(pg_trigger, &triggerTuple->t_self, triggerTuple);
                heap_freetuple(triggerTuple);
            }
        }
        systable_endscan(scan);
    }

    // Move comments from old to new index
    Relation description = table_open(DescriptionRelationId, RowExclusiveLock);
    ScanKeyData skey[3];
    ScanKeyInit(&skey[0], Anum_pg_description_objoid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oldIndexId));
    ScanKeyInit(&skey[1], Anum_pg_description_classoid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&skey[2], Anum_pg_description_objsubid,
                BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(0));

    SysScanDesc sd = systable_beginscan(description, DescriptionObjIndexId, true, NULL, 3, skey);
    HeapTuple tuple;
    while ((tuple = systable_getnext(sd)) != NULL) {
        Datum values[Natts_pg_description] = {0};
        bool nulls[Natts_pg_description] = {0};
        bool replaces[Natts_pg_description] = {0};

        values[Anum_pg_description_objoid - 1] = ObjectIdGetDatum(newIndexId);
        replaces[Anum_pg_description_objoid - 1] = true;

        tuple = heap_modify_tuple(tuple, RelationGetDescr(description),
                                 values, nulls, replaces);
        CatalogTupleUpdate(description, &tuple->t_self, tuple);
        break;  // Assume only one match
    }
    systable_endscan(sd);
    table_close(description, NoLock);

    // Handle partition inheritance
    if (get_rel_relispartition(oldIndexId)) {
        List *ancestors = get_partition_ancestors(oldIndexId);
        Oid parentIndexRelid = linitial_oid(ancestors);
        DeleteInheritsTuple(oldIndexId, parentIndexRelid, false, NULL);
        StoreSingleInheritance(newIndexId, parentIndexRelid, 1);
        list_free(ancestors);
    }

    // Swap all dependencies between old and new indexes
    changeDependenciesOf(RelationRelationId, newIndexId, oldIndexId);
    changeDependenciesOn(RelationRelationId, newIndexId, oldIndexId);
    changeDependenciesOf(RelationRelationId, oldIndexId, newIndexId);
    changeDependenciesOn(RelationRelationId, oldIndexId, newIndexId);

    // Copy statistics from old to new index
    pgstat_copy_relation_stats(newClassRel, oldClassRel);
    CopyStatistics(oldIndexId, newIndexId);

    // Clean up
    table_close(pg_class, RowExclusiveLock);
    table_close(pg_index, RowExclusiveLock);
    table_close(pg_constraint, RowExclusiveLock);
    table_close(pg_trigger, RowExclusiveLock);

    relation_close(oldClassRel, NoLock);
    relation_close(newClassRel, NoLock);
}
```