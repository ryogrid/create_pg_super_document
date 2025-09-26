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
- : The relation whose replica identity attribute bitmap is requested

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
- The returned bitmap should be freed with  when no longer needed