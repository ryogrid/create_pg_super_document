# gistxlogPageSplit

## Location
[src/include/access/gistxlog.h:68-80](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gistxlog.h#L68-L80)

## Overview
The  structure represents a WAL record for GiST index page split operations, capturing all necessary information to replay page splits during recovery.

## Definition

```c
typedef struct gistxlogPageSplit
{
	BlockNumber origrlink;		/* rightlink of the page before split */
	GistNSN		orignsn;		/* NSN of the page before split */
	bool		origleaf;		/* was split page a leaf page? */

	uint16		npage;			/* # of pages in the split */
	bool		markfollowright;	/* set F_FOLLOW_RIGHT flags */

	/*
	 * follow: 1. gistxlogPage and array of IndexTupleData per page
	 */
} gistxlogPageSplit;
```
## Detailed Description
This structure is used to log GiST index page split operations in the write-ahead log. Page splits occur when a GiST page becomes full and needs to be divided into multiple pages. The structure contains metadata about the original page state before the split, the number of resulting pages, and control flags. The actual page data follows this header structure as gistxlogPage structures and arrays of IndexTupleData for each split page.

## Parameters / Member Variables
- : Block number of the right link of the original page before the split occurred
- : GiST NSN (Node Sequence Number) of the original page before the split, used for concurrency control
- : Boolean flag indicating whether the original split page was a leaf page
- : Number of pages created in this split operation
- : Boolean flag indicating whether to set F_FOLLOW_RIGHT flags on the split pages

## Dependencies
- Functions called/Symbols referenced:
  - GistNSN
- Called from (representative examples):
  - [gistRedoPageSplitRecord](gistRedoPageSplitRecord.md)
  - [gistXLogSplit](gistXLogSplit.md)
  - [out_gistxlogPageSplit](../o/out_gistxlogPageSplit.md)
  - [gist_desc](gist_desc.md)

## Notes and Other Information
- Backup block 0 contains the left half of the split if this operation completes a page split by inserting a downlink
- Backup blocks 1 through npage contain the split pages, with block 1 being the original page
- The structure is followed by gistxlogPage structures and IndexTupleData arrays for each page in the split
- The F_FOLLOW_RIGHT flag is used to handle concurrent searches during page splits
- Page splits are complex operations that may involve multiple pages and require careful handling of concurrent access