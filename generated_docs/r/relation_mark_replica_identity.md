# relation_mark_replica_identity

## Location
[src/backend/commands/tablecmds.c:16672-16759](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L16672-L16759)

## Overview
Updates a table's replica identity configuration by modifying the relreplident field and managing per-index indisreplident flags to control logical replication behavior.

## Definition
```c
static void relation_mark_replica_identity(Relation rel, char ri_type, Oid indexOid, bool is_internal)
```

## Detailed Description
relation_mark_replica_identity is responsible for updating a table's replica identity settings, which determine how PostgreSQL identifies rows for logical replication purposes. The function performs two main operations:

1. **Updates pg_class.relreplident**: Sets the table's overall replica identity type (NOTHING, DEFAULT, USING INDEX, or FULL)
2. **Manages index flags**: Updates the indisreplident flag for all indexes on the table, ensuring only the specified index (if any) is marked as the replica identity index

The function handles all replica identity types but requires special handling when ri_type is REPLICA_IDENTITY_INDEX, where indexOid must specify a valid, suitable index. For other types, indexOid should be InvalidOid.

The function ensures transactional consistency by updating both catalog tables (pg_class and pg_index) and invalidating the relation cache to ensure all sessions see the updated replica identity configuration before performing any UPDATE or DELETE operations.

## Parameters / Member Variables
- `rel`: The relation whose replica identity is being updated
- `ri_type`: The replica identity type (character: 'd' for default, 'n' for nothing, 'f' for full, 'i' for index)
- `indexOid`: OID of the index to use as replica identity (required for REPLICA_IDENTITY_INDEX, InvalidOid otherwise)
- `is_internal`: Boolean flag indicating whether this is an internal operation (affects hook invocation)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheCopy1: Retrieves catalog tuple copies for modification
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates modified tuples in catalog tables
  - [RelationGetIndexList](../R/RelationGetIndexList.md): Gets list of all indexes on the relation
  - InvokeObjectPostAlterHookArg: Triggers post-alter hooks with additional arguments
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md): Invalidates relation cache entries
  - [heap_freetuple](../h/heap_freetuple.md): Frees heap tuple memory
  - Form_pg_class: Structure for pg_class catalog entries
  - Form_pg_index: Structure for pg_index catalog entries

- Called from (representative examples):
  - [ATExecReplicaIdentity](../A/ATExecReplicaIdentity.md): Main ALTER TABLE REPLICA IDENTITY command handler

## Notes and Other Information
- The caller must hold an exclusive lock on the relation to prevent concurrent modifications
- The function updates both pg_class and pg_index catalogs transactionally
- Cache invalidation ensures all sessions refresh replica identity settings before DML operations
- The indisreplident flag is set only for the specified index and cleared for all others
- Post-alter hooks are invoked for each modified index with the is_internal parameter
- Error handling includes cache lookup failures for both relations and indexes
- The function is optimized to only update catalog entries when values actually change

## Simplified Source

```c
static void relation_mark_replica_identity(Relation rel, char ri_type,
                                         Oid indexOid, bool is_internal) {
    // Update the table's replica identity type in pg_class
    Relation pg_class = table_open(RelationRelationId, RowExclusiveLock);
    HeapTuple pg_class_tuple = SearchSysCacheCopy1(RELOID,
                                     ObjectIdGetDatum(RelationGetRelid(rel)));

    if (!HeapTupleIsValid(pg_class_tuple))
        elog(ERROR, "cache lookup failed for relation");

    Form_pg_class pg_class_form = (Form_pg_class) GETSTRUCT(pg_class_tuple);

    // Update replica identity type if it changed
    if (pg_class_form->relreplident != ri_type) {
        pg_class_form->relreplident = ri_type;
        CatalogTupleUpdate(pg_class, &pg_class_tuple->t_self, pg_class_tuple);
    }

    table_close(pg_class, RowExclusiveLock);
    heap_freetuple(pg_class_tuple);

    // Update per-index replica identity flags
    Relation pg_index = table_open(IndexRelationId, RowExclusiveLock);

    foreach(index, RelationGetIndexList(rel)) {
        Oid thisIndexOid = lfirst_oid(index);
        bool dirty = false;

        HeapTuple pg_index_tuple = SearchSysCacheCopy1(INDEXRELID,
                                         ObjectIdGetDatum(thisIndexOid));
        if (!HeapTupleIsValid(pg_index_tuple))
            elog(ERROR, "cache lookup failed for index");

        Form_pg_index pg_index_form = (Form_pg_index) GETSTRUCT(pg_index_tuple);

        // Set flag for specified index, clear for others
        if (thisIndexOid == indexOid) {
            if (!pg_index_form->indisreplident) {
                pg_index_form->indisreplident = true;
                dirty = true;
            }
        } else {
            if (pg_index_form->indisreplident) {
                pg_index_form->indisreplident = false;
                dirty = true;
            }
        }

        // Update catalog and invalidate cache if changed
        if (dirty) {
            CatalogTupleUpdate(pg_index, &pg_index_tuple->t_self, pg_index_tuple);
            InvokeObjectPostAlterHookArg(IndexRelationId, thisIndexOid, 0,
                                       InvalidOid, is_internal);
            CacheInvalidateRelcache(rel);
        }

        heap_freetuple(pg_index_tuple);
    }

    table_close(pg_index, RowExclusiveLock);
}
```