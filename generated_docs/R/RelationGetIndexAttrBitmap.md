# RelationGetIndexAttrBitmap

## Location
[src/backend/utils/cache/relcache.c:5249-5521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L5249-L5521)

## Overview
Retrieves a bitmap of attribute numbers for columns used in indexes on a relation, with different bitmap types for specific use cases like foreign keys, primary keys, replica identity, HOT blocking, and summarized indexes.

## Definition

```c
Bitmapset *
RelationGetIndexAttrBitmap(Relation relation, IndexAttrBitmapKind attrKind)
```
## Detailed Description
This function analyzes all indexes on a given relation and returns a bitmap indicating which table attributes (columns) are involved in indexes, depending on the specified  parameter. The function supports multiple bitmap types:

- **INDEX_ATTR_BITMAP_KEY**: Columns in non-partial unique indexes not in expressions (usable for foreign keys)
- **INDEX_ATTR_BITMAP_PRIMARY_KEY**: Columns in the table's primary key (even if deferrable)
- **INDEX_ATTR_BITMAP_IDENTITY_KEY**: Columns in the table's replica identity index (empty if FULL replica identity)
- **INDEX_ATTR_BITMAP_HOT_BLOCKING**: Columns that block updates from being HOT (Heap-Only Tuples)
- **INDEX_ATTR_BITMAP_SUMMARIZED**: Columns included in summarizing indexes

The function caches results in the relation cache for performance and handles concurrent index operations by detecting changes in the index list and restarting computation if necessary. Attribute numbers are offset by  to include system attributes like OID.

The implementation considers all indexes returned by , including those not yet ready or valid, which is important for HOT-safety decisions during concurrent index operations.

## Parameters / Member Variables
- : The relation whose index attribute bitmap is requested
- : The type of attribute bitmap to return (key, primary key, identity key, HOT blocking, or summarized)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetForm
  - [RelationGetIndexList](RelationGetIndexList.md)
  - [index_open](../i/index_open.md)/index_close
  - [heap_getattr](../h/heap_getattr.md)
  - [GetPgIndexDescriptor](../G/GetPgIndexDescriptor.md)
  - [stringToNode](../s/stringToNode.md)/TextDatumGetCString
  - [bms_add_member](../b/bms_add_member.md)/bms_copy/bms_free
  - [pull_varattnos](../p/pull_varattnos.md)
  - FirstLowInvalidHeapAttributeNumber
- Called from (representative examples):
  - [heap_update](../h/heap_update.md)
  - [ExtractReplicaIdentity](../E/ExtractReplicaIdentity.md)
  - [pub_rf_contains_invalid_column](../p/pub_rf_contains_invalid_column.md)
  - [pub_collist_contains_invalid_column](../p/pub_collist_contains_invalid_column.md)
  - [GetParentedForeignKeyRefs](../G/GetParentedForeignKeyRefs.md)
  - [ExecUpdateLockMode](../E/ExecUpdateLockMode.md)
  - [logicalrep_rel_mark_updatable](../l/logicalrep_rel_mark_updatable.md)

## Notes and Other Information
- Requires at least RowExclusiveLock on the target relation to ensure deadlock-free index access
- Results are cached in the relation cache entry (, , etc.) for performance
- Handles concurrent index operations by detecting changes and restarting computation
- Differentiates between key and non-key columns in covering indexes
- Summarizing indexes don't block HOT updates but still need to be updated when column values change
- The returned bitmap is allocated in the caller's memory context and should be freed with bms_free

## Simplified Source

```c
Bitmapset *
RelationGetIndexAttrBitmap(Relation relation, IndexAttrBitmapKind attrKind)
{
    Bitmapset *uindexattrs = NULL;     /* unique index columns */
    Bitmapset *pkindexattrs = NULL;    /* primary key columns */
    Bitmapset *idindexattrs = NULL;    /* replica identity columns */
    Bitmapset *hotblockingattrs = NULL; /* HOT blocking columns */
    Bitmapset *summarizedattrs = NULL;  /* summarized index columns */
    List *indexoidlist;
    Oid relpkindex, relreplindex;

    // Return cached result if available
    if (relation->rd_attrsvalid) {
        switch (attrKind) {
            case INDEX_ATTR_BITMAP_KEY:
                return bms_copy(relation->rd_keyattr);
            case INDEX_ATTR_BITMAP_PRIMARY_KEY:
                return bms_copy(relation->rd_pkattr);
            case INDEX_ATTR_BITMAP_IDENTITY_KEY:
                return bms_copy(relation->rd_idattr);
            case INDEX_ATTR_BITMAP_HOT_BLOCKING:
                return bms_copy(relation->rd_hotblockingattr);
            case INDEX_ATTR_BITMAP_SUMMARIZED:
                return bms_copy(relation->rd_summarizedattr);
        }
    }

    // Fast path if no indexes
    if (!RelationGetForm(relation)->relhasindex)
        return NULL;

restart:
    // Get list of index OIDs
    indexoidlist = RelationGetIndexList(relation);
    if (indexoidlist == NIL)
        return NULL;

    // Cache key index OIDs before processing
    relpkindex = relation->rd_pkindex;
    relreplindex = relation->rd_replidindex;

    // Process each index
    foreach(ListCell *l, indexoidlist) {
        Oid indexOid = lfirst_oid(l);
        Relation indexDesc = index_open(indexOid, AccessShareLock);

        // Determine index characteristics
        bool isKey = indexDesc->rd_index->indisunique &&
                     /* no expressions */ && /* no predicate */;
        bool isPK = (indexOid == relpkindex);
        bool isIDKey = (indexOid == relreplindex);

        // Choose target bitmap based on index type
        Bitmapset **attrs = indexDesc->rd_indam->amsummarizing ?
                           &summarizedattrs : &hotblockingattrs;

        // Collect attribute numbers from index
        for (int i = 0; i < indexDesc->rd_index->indnatts; i++) {
            int attrnum = indexDesc->rd_index->indkey.values[i];
            if (attrnum != 0) {
                // Add to appropriate bitmaps
                *attrs = bms_add_member(*attrs,
                    attrnum - FirstLowInvalidHeapAttributeNumber);

                if (isKey && i < indexDesc->rd_index->indnkeyatts)
                    uindexattrs = bms_add_member(uindexattrs,
                        attrnum - FirstLowInvalidHeapAttributeNumber);
                if (isPK && i < indexDesc->rd_index->indnkeyatts)
                    pkindexattrs = bms_add_member(pkindexattrs,
                        attrnum - FirstLowInvalidHeapAttributeNumber);
                if (isIDKey && i < indexDesc->rd_index->indnkeyatts)
                    idindexattrs = bms_add_member(idindexattrs,
                        attrnum - FirstLowInvalidHeapAttributeNumber);
            }
        }

        // Collect attributes from expressions and predicates
        pull_varattnos(indexExpressions, 1, attrs);
        pull_varattnos(indexPredicate, 1, attrs);

        index_close(indexDesc, AccessShareLock);
    }

    // Check for concurrent changes and restart if needed
    if (index_list_changed()) {
        // Cleanup and restart
        goto restart;
    }

    // Cache results in relation
    relation->rd_keyattr = bms_copy(uindexattrs);
    relation->rd_pkattr = bms_copy(pkindexattrs);
    relation->rd_idattr = bms_copy(idindexattrs);
    relation->rd_hotblockingattr = bms_copy(hotblockingattrs);
    relation->rd_summarizedattr = bms_copy(summarizedattrs);
    relation->rd_attrsvalid = true;

    // Return requested bitmap type
    switch (attrKind) {
        case INDEX_ATTR_BITMAP_KEY:
            return uindexattrs;
        case INDEX_ATTR_BITMAP_PRIMARY_KEY:
            return pkindexattrs;
        case INDEX_ATTR_BITMAP_IDENTITY_KEY:
            return idindexattrs;
        case INDEX_ATTR_BITMAP_HOT_BLOCKING:
            return hotblockingattrs;
        case INDEX_ATTR_BITMAP_SUMMARIZED:
            return summarizedattrs;
    }
}
``` 