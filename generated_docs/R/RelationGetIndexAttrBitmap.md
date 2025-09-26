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
- The returned bitmap is allocated in the caller's memory context and should be freed with 