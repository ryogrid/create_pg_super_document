# BTDedupStateData

## Location
[src/include/access/nbtree.h:865-891](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L865-L891)

## Overview
BTDedupStateData is a comprehensive working area structure used during B-tree deduplication operations to track the state of a whole-page deduplication pass and manage pending posting lists.

## Definition

```c
typedef struct BTDedupStateData
{
	/* Deduplication status info for entire pass over page */
	bool		deduplicate;	/* Still deduplicating page? */
	int			nmaxitems;		/* Number of max-sized tuples so far */
	Size		maxpostingsize; /* Limit on size of final tuple */

	/* Metadata about base tuple of current pending posting list */
	IndexTuple	base;			/* Use to form new posting list */
	OffsetNumber baseoff;		/* page offset of base */
	Size		basetupsize;	/* base size without original posting list */

	/* Other metadata about pending posting list */
	ItemPointer htids;			/* Heap TIDs in pending posting list */
	int			nhtids;			/* Number of heap TIDs in htids array */
	int			nitems;			/* Number of existing tuples/line pointers */
	Size		phystupsize;	/* Includes line pointer overhead */

	/*
	 * Array of tuples to go on new version of the page.  Contains one entry
	 * for each group of consecutive items.  Note that existing tuples that
	 * will not become posting list tuples do not appear in the array (they
	 * are implicitly unchanged by deduplication pass).
	 */
	int			nintervals;		/* current number of intervals in array */
	BTDedupInterval intervals[MaxIndexTuplesPerPage];
} BTDedupStateData;
```
## Detailed Description
BTDedupStateData serves as a comprehensive state management structure for B-tree deduplication operations. It tracks both the overall progress of a deduplication pass across an entire page and the specific details of the current pending posting list being constructed.

The structure manages the complex process of identifying groups of duplicate tuples and combining them into posting list tuples to save space. It tracks physical size calculations to determine space savings, manages the heap TID arrays that form the core of posting lists, and maintains an array of intervals representing groups of consecutive items to be processed.

The deduplication process involves examining tuples on a page, identifying ranges of duplicates, and creating new posting list tuples that combine multiple identical key values with different heap TIDs into a single, more compact tuple.

## Parameters / Member Variables
- `deduplicate`: Boolean flag indicating whether the page is still being deduplicated
- `nmaxitems`: Counter for the number of max-sized tuples encountered so far
- `maxpostingsize`: Size limit for the final posting list tuple
- `base`: IndexTuple used as the base to form the new posting list
- `baseoff`: Page offset number of the base tuple
- `basetupsize`: Size of the base tuple without its original posting list
- `htids`: Array of heap TIDs that will comprise the pending posting list
- `nhtids`: Number of heap TIDs currently in the htids array
- `nitems`: Number of existing tuples/line pointers being consolidated
- `phystupsize`: Physical tuple size including line pointer overhead
- `nintervals`: Current number of intervals in the intervals array
- `intervals[MaxIndexTuplesPerPage]`: Array of BTDedupInterval structures representing groups of consecutive items
## Dependencies
- Functions called/Symbols referenced:
  - MaxIndexTuplesPerPage (constant)
  - [BTDedupInterval](BTDedupInterval.md) (type)
  - [IndexTuple](../I/IndexTuple.md) (type)
  - OffsetNumber (type)
  - Size (type)
  - ItemPointer (type)
- Called from (representative examples):
  - [_bt_dedup_pass](../b/_bt_dedup_pass.md)
  - [_bt_bottomupdel_pass](../b/_bt_bottomupdel_pass.md)
  - [_bt_load](../b/_bt_load.md)
  - [btree_xlog_dedup](../b/btree_xlog_dedup.md)
  - BTDedupState (typedef alias)

## Notes and Other Information
- Central to PostgreSQL's B-tree space optimization through tuple deduplication
- Manages complex state transitions during the deduplication process
- The intervals array tracks groups of consecutive items for efficient batch processing
- Physical size tracking enables accurate calculation of space savings
- Used in both regular deduplication operations and during index builds
- Essential for WAL logging and recovery of deduplication operations
- The structure supports both creating new posting lists and extending existing ones
- Designed to handle the complexity of mixed regular and posting list tuple scenarios