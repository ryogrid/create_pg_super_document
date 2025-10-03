# create_toast_table

## Location
[src/backend/catalog/toasting.c:127-400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/toasting.c#L127-L400)

## Overview
Creates a TOAST (The Oversized-Attribute Storage Technique) table and its associated index for a given relation to handle storage of large attribute values that exceed PostgreSQL's page size limits.

## Definition

```c
static bool
create_toast_table(Relation rel, Oid toastOid, Oid toastIndexOid,
				   Datum reloptions, LOCKMODE lockmode, bool check,
				   Oid OIDOldToast)
```
## Detailed Description
This is the internal workhorse function for creating TOAST tables in PostgreSQL. It performs comprehensive setup of a TOAST table structure including:

1. **Validation**: Checks if the relation already has a TOAST table and whether one is actually needed
2. **Binary Upgrade Handling**: Special logic for pg_upgrade scenarios to maintain consistency with old cluster TOAST table presence
3. **Table Creation**: Creates the TOAST table with a 3-column structure (chunk_id, chunk_seq, chunk_data)
4. **Index Creation**: Creates a unique btree index on (chunk_id, chunk_seq) for efficient chunk retrieval
5. **Catalog Updates**: Updates the parent table's pg_class entry to reference the new TOAST table
6. **Dependency Registration**: Establishes dependency relationships so TOAST table is dropped when parent is dropped

The function handles both normal operation mode and bootstrap mode with different update strategies for catalog modifications. It also manages proper namespace assignment (pg_toast for regular tables, temp-toast namespace for temporary tables) and ensures TOAST tables inherit sharing and mapping properties from their parent relations.

## Parameters / Member Variables
- `rel`: The relation (table) for which to create a TOAST table, must be already opened and locked
- `toastOid`: OID to assign to the TOAST table (normally InvalidOid except during bootstrap)
- `toastIndexOid`: OID to assign to the TOAST table's index (normally InvalidOid except during bootstrap)
- `reloptions`: Relation options (storage parameters) to apply to the TOAST table
- `lockmode`: Lock mode held on the parent relation (should be AccessExclusiveLock for safety)
- `check`: Whether to verify that the lockmode is sufficient (performs safety check)
- `OIDOldToast`: OID of the old TOAST table during binary upgrade operations
## Dependencies
- Functions called/Symbols referenced:
  - [needs_toast_table](../n/needs_toast_table.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)  
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md)
  - [table_relation_toast_am](../t/table_relation_toast_am.md)
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)
  - [index_create](../i/index_create.md)
  - SearchSysCacheCopy1
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [systable_inplace_update_begin](../s/systable_inplace_update_begin.md)
  - [systable_inplace_update_finish](../s/systable_inplace_update_finish.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
- Called from (representative examples):
  - [CheckAndCreateToastTable](../C/CheckAndCreateToastTable.md)
  - [BootstrapToastTable](../B/BootstrapToastTable.md)

## Notes and Other Information
- Returns true if a TOAST table was created, false if one already existed or wasn't needed
- The TOAST table uses a fixed 3-column schema: chunk_id (OID), chunk_seq (int4), chunk_data (bytea)
- All TOAST table columns use PLAIN storage to prevent recursive toasting
- The unique index on (chunk_id, chunk_seq) serves both uniqueness enforcement and query optimization
- During bootstrap mode, uses in-place catalog updates instead of transactional updates
- Handles special cases for shared relations, temporary relations, and binary upgrade scenarios
- The function includes extensive comments explaining the rationale for the two-column index design

## Simplified Source
```c
static bool create_toast_table(Relation rel, Oid toastOid, Oid toastIndexOid,
                              Datum reloptions, LOCKMODE lockmode, bool check,
                              Oid OIDOldToast) {
    Oid relOid = RelationGetRelid(rel);
    HeapTuple reltup;
    TupleDesc tupdesc;
    bool shared_relation, mapped_relation;
    Relation toast_rel, class_rel;
    Oid toast_relid, namespaceid;
    char toast_relname[NAMEDATALEN], toast_idxname[NAMEDATALEN];
    IndexInfo *indexInfo;
    ObjectAddress baseobject, toastobject;

    // Check if already toasted
    if (rel->rd_rel->reltoastrelid != InvalidOid)
        return false;

    // Check if TOAST table is needed
    if (!IsBinaryUpgrade) {
        if (!needs_toast_table(rel))
            return false;
    } else {
        // Binary upgrade mode - only create if old cluster had one
        if (!OidIsValid(binary_upgrade_next_toast_pg_class_oid))
            return false;
    }

    // Validate lock mode
    if (check && lockmode != AccessExclusiveLock)
        elog(ERROR, "AccessExclusiveLock required to add toast table.");

    // Generate TOAST table and index names
    snprintf(toast_relname, sizeof(toast_relname), "pg_toast_%u", relOid);
    snprintf(toast_idxname, sizeof(toast_idxname), "pg_toast_%u_index", relOid);

    // Create tuple descriptor for TOAST table (3 columns)
    tupdesc = CreateTemplateTupleDesc(3);
    TupleDescInitEntry(tupdesc, 1, "chunk_id", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, 2, "chunk_seq", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, 3, "chunk_data", BYTEAOID, -1, 0);

    // Prevent recursive toasting by setting PLAIN storage
    TupleDescAttr(tupdesc, 0)->attstorage = TYPSTORAGE_PLAIN;
    TupleDescAttr(tupdesc, 1)->attstorage = TYPSTORAGE_PLAIN;
    TupleDescAttr(tupdesc, 2)->attstorage = TYPSTORAGE_PLAIN;

    // Disable compression for TOAST table columns
    TupleDescAttr(tupdesc, 0)->attcompression = InvalidCompressionMethod;
    TupleDescAttr(tupdesc, 1)->attcompression = InvalidCompressionMethod;
    TupleDescAttr(tupdesc, 2)->attcompression = InvalidCompressionMethod;

    // Determine namespace (pg_toast or temp-toast)
    if (isTempOrTempToastNamespace(rel->rd_rel->relnamespace))
        namespaceid = GetTempToastNamespace();
    else
        namespaceid = PG_TOAST_NAMESPACE;

    // Inherit sharing and mapping properties from parent
    shared_relation = rel->rd_rel->relisshared;
    mapped_relation = RelationIsMapped(rel);

    // Create the TOAST table
    toast_relid = heap_create_with_catalog(toast_relname, namespaceid,
                                          rel->rd_rel->reltablespace, toastOid,
                                          InvalidOid, InvalidOid, rel->rd_rel->relowner,
                                          table_relation_toast_am(rel), tupdesc, NIL,
                                          RELKIND_TOASTVALUE, rel->rd_rel->relpersistence,
                                          shared_relation, mapped_relation,
                                          ONCOMMIT_NOOP, reloptions, false, true, true,
                                          OIDOldToast, NULL);

    CommandCounterIncrement();
    toast_rel = table_open(toast_relid, ShareLock);

    // Create unique index on (chunk_id, chunk_seq)
    indexInfo = makeNode(IndexInfo);
    indexInfo->ii_NumIndexAttrs = 2;
    indexInfo->ii_NumIndexKeyAttrs = 2;
    indexInfo->ii_IndexAttrNumbers[0] = 1;
    indexInfo->ii_IndexAttrNumbers[1] = 2;
    indexInfo->ii_Unique = true;

    index_create(toast_rel, toast_idxname, toastIndexOid, InvalidOid,
                 InvalidOid, InvalidOid, indexInfo,
                 list_make2("chunk_id", "chunk_seq"), BTREE_AM_OID,
                 rel->rd_rel->reltablespace, /* other params */, INDEX_CREATE_IS_PRIMARY,
                 0, true, true, NULL);

    table_close(toast_rel, NoLock);

    // Update parent table's pg_class entry to reference TOAST table
    class_rel = table_open(RelationRelationId, RowExclusiveLock);

    if (!IsBootstrapProcessingMode()) {
        // Normal transactional update
        reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relOid));
        ((Form_pg_class) GETSTRUCT(reltup))->reltoastrelid = toast_relid;
        CatalogTupleUpdate(class_rel, &reltup->t_self, reltup);
    } else {
        // Bootstrap mode - in-place update
        ScanKeyData key[1];
        void *state;

        ScanKeyInit(&key[0], Anum_pg_class_oid, BTEqualStrategyNumber,
                   F_OIDEQ, ObjectIdGetDatum(relOid));
        systable_inplace_update_begin(class_rel, ClassOidIndexId, true,
                                     NULL, 1, key, &reltup, &state);
        ((Form_pg_class) GETSTRUCT(reltup))->reltoastrelid = toast_relid;
        systable_inplace_update_finish(state, reltup);
    }

    heap_freetuple(reltup);
    table_close(class_rel, RowExclusiveLock);

    // Register dependency from TOAST table to main table
    if (!IsBootstrapProcessingMode()) {
        baseobject.classId = RelationRelationId;
        baseobject.objectId = relOid;
        baseobject.objectSubId = 0;
        toastobject.classId = RelationRelationId;
        toastobject.objectId = toast_relid;
        toastobject.objectSubId = 0;

        recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);
    }

    CommandCounterIncrement();
    return true;
}
```