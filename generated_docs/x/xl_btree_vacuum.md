# xl_btree_vacuum

## Location
[src/include/access/nbtxlog.h:223-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtxlog.h#L223-L235)

## Overview
The xl_btree_vacuum structure represents a WAL record for B-tree tuple deletion operations performed by VACUUM, supporting both complete deletion and partial updates of posting list tuples.

## Definition
```c
typedef struct xl_btree_vacuum
{
    uint16      ndeleted;
    uint16      nupdated;
    
    /*----
     * In payload of blk 0 :
     * - DELETED TARGET OFFSET NUMBERS
     * - UPDATED TARGET OFFSET NUMBERS  
     * - UPDATED TUPLES METADATA (xl_btree_update) ITEMS
     *----
     */
} xl_btree_vacuum;
```

## Detailed Description
This structure logs deletions of index tuples on leaf pages during VACUUM operations. It supports two types of operations: complete deletion of index tuples and updates to posting list tuples where only a subset of TIDs (Tuple IDs) are removed. This dual functionality allows VACUUM to efficiently handle both simple index tuple removal and complex posting list modifications.

The record includes metadata about the number of deleted and updated tuples, followed by payload data containing the offset numbers of affected tuples and metadata for updated posting lists. When updating posting lists, the record uses xl_btree_update entries that, combined with the original tuple data, allow recovery to reconstruct the final updated tuple with the remaining TIDs.

This record type is similar to xl_btree_delete but lacks conflict horizon fields since VACUUM can rely on conflicts generated during earlier table pruning operations.

## Parameters / Member Variables
- `ndeleted`: Number of index tuples to be completely deleted from the page
- `nupdated`: Number of posting list tuples to be updated (partial TID removal)

## Dependencies
- Functions called/Symbols referenced:
  - uint16 (type)
  - [xl_btree_update](xl_btree_update.md) (metadata structure for updated tuples)

- Called from (representative examples):
  - [_bt_delitems_vacuum](../b/_bt_delitems_vacuum.md) (src/backend/access/nbtree/nbtpage.c:1230)
  - [btree_xlog_vacuum](../b/btree_xlog_vacuum.md) (src/backend/access/nbtree/nbtxlog.c:601)
  - [btree_desc](../b/btree_desc.md) (src/backend/access/rmgrdesc/nbtdesc.c:60)
  - SizeOfBtreeVacuum (src/include/access/nbtxlog.h:237)

## Notes and Other Information
- Used specifically by VACUUM operations, not ad-hoc deletions (which use xl_btree_delete)
- Supports both complete tuple deletion and partial posting list updates
- Updates are only used when TIDs remain after deletion; otherwise tuples are deleted outright
- Payload structure: deleted offset numbers, updated offset numbers, then xl_btree_update metadata
- Recovery uses xl_btree_update entries with original tuple data to reconstruct final updated tuples
- No conflict horizon fields needed since VACUUM relies on earlier pruning conflicts
- Operates only on leaf pages where actual index tuples and posting lists reside
- Efficient handling of posting list modifications without requiring complete tuple reconstruction