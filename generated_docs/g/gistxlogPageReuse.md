# gistxlogPageReuse

## Location
[src/include/access/gistxlog.h:99-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gistxlog.h#L99-L106)

## Overview
The  structure represents a WAL record for GiST index page reuse operations, containing information necessary for hot standby servers to handle recovery conflicts when previously deleted pages are reused.

## Definition

```c
typedef struct gistxlogPageReuse
{
	RelFileLocator locator;
	BlockNumber block;
	FullTransactionId snapshotConflictHorizon;
	bool		isCatalogRel;	/* to handle recovery conflict during logical
								 * decoding on standby */
} gistxlogPageReuse;
```
## Detailed Description
This structure is used to log GiST index page reuse operations in the write-ahead log. Page reuse occurs when a previously deleted page is recycled for new data. This information is particularly important for hot standby servers to properly handle recovery conflicts, ensuring that transactions with older snapshots don't access reused pages that might contain different data than what they expect to see.

## Parameters / Member Variables
- : RelFileLocator identifying the specific relation file where the page reuse is occurring
- : Block number of the page being reused within the relation
- : Full transaction ID representing the conflict horizon for snapshot conflicts during recovery
- : Boolean flag indicating if this is a catalog relation, used for handling recovery conflicts during logical decoding on standby servers

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionId
- Called from (representative examples):
  - [gistRedoPageReuse](gistRedoPageReuse.md)
  - [gistXLogPageReuse](gistXLogPageReuse.md)
  - [out_gistxlogPageReuse](../o/out_gistxlogPageReuse.md)
  - [gist_desc](gist_desc.md)
  - SizeOfGistxlogPageReuse

## Notes and Other Information
- This WAL record type is specifically designed for hot standby scenarios where recovery conflict handling is crucial
- Page reuse is an optimization that allows the index to recycle previously deleted pages rather than always allocating new ones
- The snapshot conflict horizon helps determine which transactions on standby servers might conflict with the page reuse
- Catalog relations require special handling during logical decoding to maintain consistency
- This record type has no associated backup blocks since it's primarily for conflict resolution rather than data recovery