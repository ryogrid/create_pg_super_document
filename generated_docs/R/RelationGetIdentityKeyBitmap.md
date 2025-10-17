# RelationGetIdentityKeyBitmap

## Location
[src/backend/utils/cache/relcache.c:5522-5595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L5522-L5595)

## Overview
Retrieves a bitmap of attribute numbers for columns that are part of the configured replica identity index, specifically designed for logical replication operations.

## Definition

```c
Bitmapset *
RelationGetIdentityKeyBitmap(Relation relation)
```
## Detailed Description
This function returns a bitmap indicating which table attributes (columns) are part of the replica identity index for a given relation. It is a specialized version of  designed specifically for logical replication use cases.

The key differences from  are:
- It operates on historic snapshots and doesn't acquire locks on indexes since it works with historical data
- It doesn't need to retry on index set changes because changes are absorbed during WAL decoding
- It only focuses on replica identity attributes, not other types of index attributes
- It excludes non-key columns from the bitmap, only including actual key columns

The function first checks if the result is already cached in . If not cached, it looks up the replica identity index using  and builds the bitmap by examining the index's key attributes. The result is cached for future use.

## Parameters / Member Variables
- `relation`: The relation whose replica identity attribute bitmap is requested
## Dependencies
- Functions called/Symbols referenced:
  - [bms_copy](../b/bms_copy.md)
  - RelationGetForm
  - [HistoricSnapshotActive](../H/HistoricSnapshotActive.md)
  - [RelationGetReplicaIndex](RelationGetReplicaIndex.md)
  - [RelationIdGetRelation](RelationIdGetRelation.md)
  - RelationIsValid
  - [bms_add_member](../b/bms_add_member.md)
  - FirstLowInvalidHeapAttributeNumber
  - [RelationClose](RelationClose.md)
  - [bms_free](../b/bms_free.md)
- Called from (representative examples):
  - [logicalrep_write_attrs](../l/logicalrep_write_attrs.md)

## Notes and Other Information
- Designed specifically for logical replication scenarios using historic snapshots
- Requires that  returns true, enforced by an assertion
- Does not acquire locks on indexes unlike 
- Caches results in  for performance
- Only includes key columns in the bitmap, excluding non-key columns from covering indexes
- Returns NULL if the relation has no indexes or no replica identity index is configured
- The returned bitmap should be freed with bms_free when no longer needed

## Simplified Source

```c
Bitmapset *RelationGetIdentityKeyBitmap(Relation relation) {
    Bitmapset *idindexattrs = NULL;
    Relation indexDesc;
    int i;
    Oid replidindex;
    MemoryContext oldcxt;

    // Return cached result if available
    if (relation->rd_idattr != NULL)
        return bms_copy(relation->rd_idattr);

    // Quick exit if no indexes
    if (!RelationGetForm(relation)->relhasindex)
        return NULL;

    // Must be using historic snapshot for logical replication
    Assert(HistoricSnapshotActive());

    // Get the replica identity index OID
    replidindex = RelationGetReplicaIndex(relation);
    if (!OidIsValid(replidindex))
        return NULL;

    // Open the replica identity index
    indexDesc = RelationIdGetRelation(replidindex);
    if (!RelationIsValid(indexDesc))
        elog(ERROR, "could not open relation with OID %u", relation->rd_replidindex);

    // Build bitmap of key attributes
    for (i = 0; i < indexDesc->rd_index->indnatts; i++) {
        int attrnum = indexDesc->rd_index->indkey.values[i];

        // Include only key columns (not included columns)
        if (attrnum != 0 && i < indexDesc->rd_index->indnkeyatts) {
            idindexattrs = bms_add_member(idindexattrs,
                                          attrnum - FirstLowInvalidHeapAttributeNumber);
        }
    }

    RelationClose(indexDesc);

    // Free old cached bitmap and cache the new one
    bms_free(relation->rd_idattr);
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
    relation->rd_idattr = bms_copy(idindexattrs);
    MemoryContextSwitchTo(oldcxt);

    // Return working copy for caller
    return idindexattrs;
}
```