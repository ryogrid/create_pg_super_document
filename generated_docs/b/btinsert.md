# btinsert

## Location
[src/backend/access/nbtree/nbtree.c:182-205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L182-L205)

## Overview
The btinsert function inserts an index tuple into a B-tree index by forming the tuple from provided values and delegating the actual insertion to the core B-tree insertion logic.

## Definition

```c
bool
btinsert(Relation rel, Datum *values, bool *isnull,
		 ItemPointer ht_ctid, Relation heapRel,
		 IndexUniqueCheck checkUnique,
		 bool indexUnchanged,
		 IndexInfo *indexInfo)
```
## Detailed Description
The btinsert function serves as the main entry point for inserting tuples into B-tree indexes. It acts as a wrapper that handles the conversion of raw column values into a properly formatted index tuple, then delegates the complex insertion logic to the internal _bt_doinsert function. The function forms an index tuple from the provided column values and null flags, sets the tuple's heap pointer to reference the corresponding heap tuple, and then performs the actual insertion while handling uniqueness constraints and other insertion policies.

This function is part of PostgreSQL's index access method interface and is called whenever a new tuple needs to be added to a B-tree index, such as during INSERT operations, index creation, or tuple updates that affect indexed columns.

## Parameters / Member Variables
- `rel`: The B-tree index relation where the tuple will be inserted
- `*values`: Array of Datum values for each indexed column
- `*isnull`: Array of boolean flags indicating which values are NULL
- `ht_ctid`: ItemPointer to the heap tuple this index entry references
- `heapRel`: The heap relation containing the actual tuple data
- `checkUnique`: Specifies how to handle uniqueness constraints during insertion
- `indexUnchanged`: Boolean indicating whether the index values have changed (optimization hint)
- `*indexInfo`: Metadata about the index structure and properties
## Dependencies
- Functions called/Symbols referenced:
  - [index_form_tuple](../i/index_form_tuple.md) (creates index tuple from values)
  - RelationGetDescr (gets relation descriptor)
  - [_bt_doinsert](_bt_doinsert.md) (performs the actual B-tree insertion)
  - [pfree](../p/pfree.md) (frees allocated memory)
  - IndexUniqueCheck, IndexInfo (type definitions)
- Called from (representative examples):
  - [bthandler](bthandler.md) (registered as aminsert callback)
  - Index maintenance operations during INSERT/UPDATE queries

## Notes and Other Information
- Returns a boolean indicating success/failure of the insertion operation
- Memory management is handled properly by freeing the constructed index tuple
- The function abstracts away the complexity of B-tree insertion from the access method interface
- Supports uniqueness checking and handles various insertion scenarios
- The indexUnchanged parameter allows for optimizations when index values haven't actually changed
- Part of the standard PostgreSQL index access method framework

## Simplified Source

```c
bool btinsert(Relation rel, Datum *values, bool *isnull,
             ItemPointer ht_ctid, Relation heapRel,
             IndexUniqueCheck checkUnique,
             bool indexUnchanged,
             IndexInfo *indexInfo) {

    // Create index tuple from provided values
    IndexTuple itup = index_form_tuple(RelationGetDescr(rel), values, isnull);
    itup->t_tid = *ht_ctid;  // Set heap tuple pointer

    // Perform the actual B-tree insertion
    bool result = _bt_doinsert(rel, itup, checkUnique, indexUnchanged, heapRel);

    // Clean up allocated tuple
    pfree(itup);

    return result;
}
```