# _bt_mkscankey

## Location
[src/backend/access/nbtree/nbtutils.c:129-220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L129-L220)

## Overview
Builds an insertion scan key that contains comparison data from an index tuple as well as comparator routines appropriate to the key datatypes, intended for use with _bt_compare() and _bt_truncate().

## Definition

```c
structed on key columns.
	 * Truncated attributes and non-key attributes are omitted from the final
	 * scan key.
	 */
	key = palloc(offsetof(BTScanInsertData, scankeys) +
				 sizeof(ScanKeyData) * indnkeyatts);
```
## Detailed Description
This function constructs a BTScanInsert structure that serves as a specialized scan key for B-tree operations. The scan key contains comparison data extracted from the provided index tuple along with appropriate comparator functions for each key datatype. The resulting structure is optimized for use in B-tree comparison operations (_bt_compare) and truncation operations (_bt_truncate).

The function handles several important cases:
- When a NULL index tuple is passed, it initializes the scankey as if an "all truncated" pivot tuple was provided
- It may need to share lock the metapage to determine if keys are expected to be unique (heapkeyspace index)
- For NULL tuple cases, it assumes heapkeyspace=true and allequalimage=false
- It handles truncated attributes and non-key attributes by omitting them from the final scan key
- In NULLS NOT DISTINCT mode, it pretends there are no null keys for full uniqueness checking

## Parameters / Member Variables
- : The index relation for which to build the scan key
- : The index tuple containing comparison data, or NULL for utility operations

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetDescr
  - IndexRelationGetNumberOfKeyAttributes  
  - BTreeTupleGetNAtts
  - IndexRelationGetNumberOfAttributes
  - [_bt_metaversion](_bt_metaversion.md)
  - [BTreeTupleGetHeapTID](../B/BTreeTupleGetHeapTID.md)
  - [index_getprocinfo](../i/index_getprocinfo.md)
  - [index_getattr](../i/index_getattr.md)
  - [ScanKeyEntryInitializeWithInfo](../S/ScanKeyEntryInitializeWithInfo.md)
  - [BTScanInsertData](../B/BTScanInsertData.md)
  - BTORDER_PROC
  - SK_ISNULL
  - SK_BT_INDOPTION_SHIFT

- Called from (representative examples):
  - [_bt_doinsert](_bt_doinsert.md) (B-tree insertion operations)
  - [_bt_pagedel](_bt_pagedel.md) (Page deletion operations)
  - [_bt_leafbuild](_bt_leafbuild.md) (Leaf page building during index creation)
  - [tuplesort_begin_cluster](../t/tuplesort_begin_cluster.md) (Cluster sorting operations)
  - [tuplesort_begin_index_btree](../t/tuplesort_begin_index_btree.md) (B-tree index sorting)

## Notes and Other Information
- The function assumes heapkeyspace index behavior when caller passes a NULL tuple, which is useful for index build operations that don't have access to the metapage yet
- Key attributes are extracted only up to the number of key attributes (indnkeyatts), excluding non-key attributes
- Truncated attributes are defensively represented as NULL values in the scan key
- The scantid field is set to the heap TID from the tuple if available and the index is heapkeyspace
- The function handles both regular insertion scenarios and utility operations that may not have complete tuple data