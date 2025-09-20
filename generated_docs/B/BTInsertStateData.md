# BTInsertStateData

## Location
[src/include/access/nbtree.h:809-833](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L809-L833)

## Overview
BTInsertStateData is a working area structure used during B-tree insertion operations to track the current position and state while performing uniqueness checks and determining the exact insertion location.

## Definition

```c
typedef struct BTInsertStateData
{
	IndexTuple	itup;			/* Item we're inserting */
	Size		itemsz;			/* Size of itup -- should be MAXALIGN()'d */
	BTScanInsert itup_key;		/* Insertion scankey */

	/* Buffer containing leaf page we're likely to insert itup on */
	Buffer		buf;

	/*
	 * Cache of bounds within the current buffer.  Only used for insertions
	 * where _bt_check_unique is called.  See _bt_binsrch_insert and
	 * _bt_findinsertloc for details.
	 */
	bool		bounds_valid;
	OffsetNumber low;
	OffsetNumber stricthigh;

	/*
	 * if _bt_binsrch_insert found the location inside existing posting list,
	 * save the position inside the list.  -1 sentinel value indicates overlap
	 * with an existing posting list tuple that has its LP_DEAD bit set.
	 */
	int			postingoff;
} BTInsertStateData;
```
## Detailed Description
BTInsertStateData serves as a comprehensive working area for B-tree insertion operations. It is populated after descending the tree to the first leaf page where the new tuple might belong. The structure maintains all necessary state information during the insertion process, particularly during uniqueness checking phases before the final insertion location is determined.

The structure includes caching mechanisms for search bounds within the current buffer, which are particularly useful for insertions that require uniqueness checking via _bt_check_unique. It also handles posting list operations, including tracking positions within existing posting lists and managing LP_DEAD tuple scenarios.

This structure is primarily used internally by nbtinsert.c but is also utilized by _bt_binsrch_insert for efficient insertion operations.

## Parameters / Member Variables
- `itup`: The IndexTuple being inserted into the B-tree
- `itemsz`: Size of the itup item, which should be MAXALIGN()'d for proper alignment
- `itup_key`: BTScanInsert structure containing the insertion scankey for the tuple
- `buf`: Buffer containing the leaf page where the tuple will likely be inserted
- `bounds_valid`: Flag indicating whether the cached bounds are valid for the current buffer
- `low`: Lower bound offset number within the current buffer (used for uniqueness checking)
- `stricthigh`: Upper bound offset number within the current buffer (used for uniqueness checking)
- `postingoff`: Position within an existing posting list if _bt_binsrch_insert found the location inside one; -1 indicates overlap with LP_DEAD tuple
## Dependencies
- Functions called/Symbols referenced:
  - BTScanInsert (type)
  - [IndexTuple](../I/IndexTuple.md) (type)
  - Size (type)
  - Buffer (type)
  - OffsetNumber (type)
- Called from (representative examples):
  - [_bt_doinsert](../b/_bt_doinsert.md)
  - BTInsertState (typedef alias)

## Notes and Other Information
- This structure is designed to be private to nbtinsert.c but is shared with _bt_binsrch_insert for efficiency
- The bounds caching mechanism (bounds_valid, low, stricthigh) is only used during insertions requiring uniqueness checks
- The postingoff field handles special cases of posting list insertions and LP_DEAD tuple management
- The structure optimizes insertion performance by maintaining state across multiple related operations during the insertion process
- Used extensively in B-tree insertion algorithms to maintain context and avoid redundant operations