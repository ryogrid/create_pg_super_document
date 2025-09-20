# GISTInsertStack

## Location
[src/include/access/gist_private.h:207-232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gist_private.h#L207-L232)

## Overview
GISTInsertStack is a structure used during GiST index insertions to maintain a stack of pages from root to leaf, tracking buffer locks and managing the descent path for tuple insertion and potential page splits.

## Definition

```c
typedef struct GISTInsertStack
{
	/* current page */
	BlockNumber blkno;
	Buffer		buffer;
	Page		page;

	/*
	 * log sequence number from page->lsn to recognize page update and compare
	 * it with page's nsn to recognize page split
	 */
	GistNSN		lsn;

	/*
	 * If set, we split the page while descending the tree to find an
	 * insertion target. It means that we need to retry from the parent,
	 * because the downlink of this page might no longer cover the new key.
	 */
	bool		retry_from_parent;

	/* offset of the downlink in the parent page, that points to this page */
	OffsetNumber downlinkoffnum;

	/* pointer to parent */
	struct GISTInsertStack *parent;
} GISTInsertStack;
```
## Detailed Description
GISTInsertStack represents a single level in the descent path during GiST index operations, forming a stack structure that tracks the path from root to target leaf page. This structure is essential for maintaining consistency during concurrent operations, as it stores LSN information to detect page modifications and splits that may have occurred during the descent. The stack enables proper retry logic when page splits invalidate the current descent path, ensuring that insertions find the correct target page even in high-concurrency scenarios.

## Parameters / Member Variables
- : BlockNumber identifying the physical block number of this page
- : Buffer handle for the locked page buffer
- : Page pointer to the actual page content
- : GistNSN (Log Sequence Number) used to detect page updates and splits by comparing with the page's NSN
- : Boolean flag indicating that a page split occurred during descent, requiring retry from the parent level
- : OffsetNumber specifying the offset of the downlink in the parent page that points to this page
- : Pointer to the parent GISTInsertStack node, forming the stack structure

## Dependencies
- Functions called/Symbols referenced:
  - GistNSN
- Called from (representative examples):
  - [gistdoinsert](../g/gistdoinsert.md)
  - [gistFindPath](../g/gistFindPath.md)
  - [gistFindCorrectParent](../g/gistFindCorrectParent.md)
  - [gistformdownlink](../g/gistformdownlink.md)
  - [gistfixsplit](../g/gistfixsplit.md)
  - [gistinserttuple](../g/gistinserttuple.md)
  - [gistinserttuples](../g/gistinserttuples.md)
  - [gistfinishsplit](../g/gistfinishsplit.md)

## Notes and Other Information
The stack structure is crucial for handling concurrent modifications in GiST indexes. The LSN tracking mechanism allows detection of page splits that occur between the time a page is first visited and when it's actually modified. The retry_from_parent flag implements an essential concurrency control mechanism, ensuring that when splits invalidate the current path, the operation can restart from an appropriate level rather than failing or producing incorrect results.