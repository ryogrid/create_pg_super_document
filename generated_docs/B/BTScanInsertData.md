# BTScanInsertData

## Location
[src/include/access/nbtree.h:784-794](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L784-L794)

## Overview
BTScanInsertData is a structure that contains btree-private state needed to find an initial position for an indexscan or to insert new tuples, implementing an "insertion scankey" for B-tree operations.

## Definition

```c
typedef struct BTScanInsertData
{
	bool		heapkeyspace;
	bool		allequalimage;
	bool		anynullkeys;
	bool		nextkey;
	bool		backward;		/* backward index scan? */
	ItemPointer scantid;		/* tiebreaker for scankeys */
	int			keysz;			/* Size of scankeys array */
	ScanKeyData scankeys[INDEX_MAX_KEYS];	/* Must appear last */
} BTScanInsertData;
```
## Detailed Description
BTScanInsertData serves as the core data structure for B-tree descent operations using _bt_search. It encapsulates all the necessary state information for both index scanning operations and tuple insertion operations. The structure is designed to handle both regular index scans and the specialized requirements of tuple insertion in heapkeyspace indexes (version 4+ indexes).

The structure incorporates several optimization flags and metadata about the index properties, such as whether the index supports deduplication and whether heap TID is used as a tiebreaker. It also maintains scan direction information and handles NULL key values appropriately.

The scankeys array serves as a flexible array member that contains scan key entries for all attributes that are compared before the heap TID tiebreaker. This design allows for efficient stack allocation while supporting the full range of possible index attributes.

## Parameters / Member Variables
- : Indicates if all keys in the index are physically unique because heap TID is used as a tiebreaker attribute, and if index may have truncated key attributes in pivot tuples
- : Set to indicate that deduplication is safe for the index (property of the index relation)
- : Indicates if any of the keys had NULL value when scankey was built from index tuple
- : Used for controlling search positioning (see _bt_first comments)
- : Flag indicating whether this is a backward index scan
- : Heap TID used as a final tiebreaker attribute; set to NULL when scan doesn't need to find a position for a specific physical tuple
- : Size of the scankeys array
- : Array of scan key entries for attributes compared before scantid (user-visible attributes)

## Dependencies
- Functions called/Symbols referenced:
  - INDEX_MAX_KEYS (constant)
  - ItemPointer (type)
  - [ScanKeyData](../S/ScanKeyData.md) (type)
- Called from (representative examples):
  - [_bt_first](../b/_bt_first.md)
  - [_bt_mkscankey](../b/_bt_mkscankey.md)
  - BTScanInsert (typedef alias)

## Notes and Other Information
- The structure is designed to support both regular index scans and tuple insertion operations
- Must contain a scan key for every attribute during insertion, but some can be omitted for regular scans
- The heapkeyspace flag corresponds to index version 4+ which supports heap TID as tiebreaker
- The scankeys array is implemented as a flexible array member but sized to allow stack allocation
- The anynullkeys flag is particularly important for unique index non-pivot tuple insertion logic
- Used extensively in B-tree search and insertion algorithms for efficient tree traversal