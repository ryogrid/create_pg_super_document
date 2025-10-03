# gistplacetopage

## Location
[src/backend/access/gist/gist.c:225-633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gist.c#L225-L633)

## Overview
A complex core function that places tuples on a GiST page, handling page splits when necessary and managing all the intricate details of GiST page structure and concurrency control.

## Definition

```c
struct the new root page with the
		 * downlinks here directly, instead of requiring the caller to insert
		 * them. Add the new root page to the list along with the child pages.
		 */
		if (is_rootsplit)
		{
			IndexTuple *downlinks;
			int			ndownlinks = 0;
			int			i;

			rootpg.buffer = buffer;
			rootpg.page = PageGetTempPageCopySpecial(BufferGetPage(rootpg.buffer));
			GistPageGetOpaque(rootpg.page)->flags = 0;

			/* Prepare a vector of all the downlinks */
			for (ptr = dist; ptr; ptr = ptr->next)
				ndownlinks++;
			downlinks = palloc(sizeof(IndexTuple) * ndownlinks);
			for (i = 0, ptr = dist; ptr; ptr = ptr->next)
				downlinks[i++] = ptr->itup;

			rootpg.block.blkno = GIST_ROOT_BLKNO;
			rootpg.block.num = ndownlinks;
			rootpg.list = gistfillitupvec(downlinks, ndownlinks,
										  &(rootpg.lenlist));
			rootpg.itup = NULL;

			rootpg.next = dist;
			dist = &rootpg;
		}
		else
		{
			/* Prepare split-info to be returned to caller */
			for (ptr = dist; ptr; ptr = ptr->next)
			{
				GISTPageSplitInfo *si = palloc(sizeof(GISTPageSplitInfo));

				si->buf = ptr->buffer;
				si->downlink = ptr->itup;
				*splitinfo = lappend(*splitinfo, si);
			}
		}

		/*
		 * Fill all pages. All the pages are new, ie. freshly allocated empty
		 * pages, or a temporary copy of the old page.
		 */
		for (ptr = dist;
```
## Detailed Description
This function is the central workhorse for placing tuples onto GiST pages. It handles both simple insertion when there's sufficient space and complex page splitting when the page is full. The function manages several critical aspects of GiST operation:

1. **Space Management**: Determines if there's enough space on the page for new tuples, considering existing tuples and any old tuple to be replaced.

2. **Page Splitting**: When insufficient space exists, performs a sophisticated split operation that creates multiple new pages, properly distributes tuples among them, and maintains page linkage.

3. **Root Splitting**: Special handling for root page splits, which creates a new root with downlinks to child pages in a single atomic operation.

4. **Concurrency Control**: Uses follow-right flags and NSN (Node Sequence Numbers) to ensure concurrent operations can navigate the tree correctly during and after splits.

5. **WAL Logging**: Properly logs all operations for crash recovery, with special handling for index build operations.

6. **Memory Management**: Operates within critical sections to ensure atomicity and handles both buffered and unbuffered operation modes.

## Parameters / Member Variables
- `r`: The GiST index relation being operated on
- `freespace`: Amount of free space that must be preserved on the page
- `giststate`: GiST state containing operator class information
- `buffer`: The target buffer/page for insertion
- `itup`: Array of index tuples to be inserted
- `ntup`: Number of tuples in the itup array
- `oldoffnum`: Offset of old tuple to replace (InvalidOffsetNumber if none)
- `newblkno`: Returns block number where first new tuple was placed
- `leftchildbuf`: Buffer of left child page (for downlink operations)
- `splitinfo`: Returns information about split pages for caller to handle
- `markfollowright`: Whether to mark left child with follow-right flag during splits
- `heapRel`: The heap relation (for predicate locking)
- `is_build`: Whether this is part of initial index build

## Dependencies
- Functions called/Symbols referenced:
  - [gistnospace](gistnospace.md) (checks if tuples fit on page)
  - [gistprunepage](gistprunepage.md) (removes dead tuples from leaf pages)
  - [gistextractpage](gistextractpage.md) (extracts existing tuples from page)
  - [gistjoinvector](gistjoinvector.md) (combines tuple vectors)
  - [gistSplit](gistSplit.md) (performs the actual page splitting algorithm)
  - [gistNewBuffer](gistNewBuffer.md) (allocates new buffers for split pages)
  - [GISTInitBuffer](../G/GISTInitBuffer.md) (initializes new GiST pages)
  - [gistfillitupvec](gistfillitupvec.md) (fills tuple vector for pages)
  - [gistfillbuffer](gistfillbuffer.md) (adds tuples to a page)
  - [gistXLogSplit](gistXLogSplit.md)/gistXLogUpdate (WAL logging functions)
  - [PageIndexTupleOverwrite](../P/PageIndexTupleOverwrite.md) (efficient tuple replacement)
  - Various page manipulation and buffer management functions
- Constants used:
  - GIST_ROOT_BLKNO (root page block number)
  - GIST_MAX_SPLIT_PAGES (maximum pages from one split)
  - F_LEAF (leaf page flag)
  - GistBuildLSN (special LSN for index builds)
- Called from:
  - [gistinserttuples](gistinserttuples.md) (at src/backend/access/gist/gist.c:1305)
  - [gistbufferinginserttuples](gistbufferinginserttuples.md) (at src/backend/access/gist/gistbuild.c:1063)

## Notes and Other Information
- Returns true if the page was split, false otherwise
- For root splits, the function handles everything atomically and releases child pages
- For non-root splits, returns split information for the caller to insert downlinks
- Uses sophisticated concurrency control with follow-right flags and NSNs
- Handles both regular operation and index build modes with different WAL strategies
- Can perform garbage collection on leaf pages before splitting
- Maintains predicate locks for serializable isolation level
- Critical sections ensure atomic updates for crash safety
- Located in src/backend/access/gist/gist.c:225-633

## Simplified Source
```c
bool gistplacetopage(Relation rel, Size freespace, GISTSTATE *giststate,
                     Buffer buffer, IndexTuple *itup, int ntup,
                     OffsetNumber oldoffnum, BlockNumber *newblkno,
                     Buffer leftchildbuf, List **splitinfo,
                     bool markfollowright, Relation heapRel, bool is_build) {
    BlockNumber blkno = BufferGetBlockNumber(buffer);
    Page page = BufferGetPage(buffer);
    bool is_leaf = GistPageIsLeaf(page);
    bool is_split;
    XLogRecPtr recptr;

    // Validate page state
    if (GistFollowRight(page))
        elog(ERROR, "concurrent GiST page split was incomplete");
    Assert(!GistPageIsDeleted(page));

    *splitinfo = NIL;

    // Check if page has enough space
    is_split = gistnospace(page, itup, ntup, oldoffnum, freespace);

    // Try garbage collection on full leaf pages
    if (is_split && is_leaf && GistPageHasGarbage(page)) {
        gistprunepage(rel, page, buffer, heapRel);
        is_split = gistnospace(page, itup, ntup, oldoffnum, freespace);
    }

    if (is_split) {
        // Page split required - complex path
        IndexTuple *itvec;
        int tlen;
        SplitPageLayout *dist;
        bool is_rootsplit = (blkno == GIST_ROOT_BLKNO);

        // Extract existing tuples and remove old one if replacing
        itvec = gistextractpage(page, &tlen);
        if (OffsetNumberIsValid(oldoffnum)) {
            // Remove old tuple from vector
            int pos = oldoffnum - FirstOffsetNumber;
            tlen--;
            if (pos != tlen)
                memmove(itvec + pos, itvec + pos + 1,
                        sizeof(IndexTuple) * (tlen - pos));
        }

        // Combine with new tuples and split
        itvec = gistjoinvector(itvec, &tlen, itup, ntup);
        dist = gistSplit(rel, page, itvec, tlen, giststate);

        // Allocate buffers for split pages
        if (!is_rootsplit) {
            // Regular split - reuse original buffer for leftmost page
            dist->buffer = buffer;
            dist->page = PageGetTempPageCopySpecial(page);
            GistPageGetOpaque(dist->page)->flags = is_leaf ? F_LEAF : 0;
        }

        // Allocate new buffers for additional pages
        for (SplitPageLayout *ptr = is_rootsplit ? dist : dist->next;
             ptr; ptr = ptr->next) {
            ptr->buffer = gistNewBuffer(rel, heapRel);
            GISTInitBuffer(ptr->buffer, is_leaf ? F_LEAF : 0);
            ptr->page = BufferGetPage(ptr->buffer);
            ptr->block.blkno = BufferGetBlockNumber(ptr->buffer);
        }

        // Set up downlink tuples
        for (SplitPageLayout *ptr = dist; ptr; ptr = ptr->next) {
            ItemPointerSetBlockNumber(&(ptr->itup->t_tid), ptr->block.blkno);
            GistTupleSetValid(ptr->itup);
        }

        // Handle root split specially
        if (is_rootsplit) {
            // Create new root with downlinks to child pages
            // (Complex root page setup omitted for brevity)
        } else {
            // Prepare split info for caller
            for (SplitPageLayout *ptr = dist; ptr; ptr = ptr->next) {
                GISTPageSplitInfo *si = palloc(sizeof(GISTPageSplitInfo));
                si->buf = ptr->buffer;
                si->downlink = ptr->itup;
                *splitinfo = lappend(*splitinfo, si);
            }
        }

        // Fill all split pages with tuples
        for (SplitPageLayout *ptr = dist; ptr; ptr = ptr->next) {
            // Add tuples to page and set up page links
            // (Page filling logic simplified)
        }

        START_CRIT_SECTION();

        // Mark buffers dirty and log operation
        for (SplitPageLayout *ptr = dist; ptr; ptr = ptr->next)
            MarkBufferDirty(ptr->buffer);

        // Write WAL record
        if (is_build)
            recptr = GistBuildLSN;
        else if (RelationNeedsWAL(rel))
            recptr = gistXLogSplit(is_leaf, dist, /* other args */);
        else
            recptr = gistGetFakeLSN(rel);

        // Set LSN on all pages
        for (SplitPageLayout *ptr = dist; ptr; ptr = ptr->next)
            PageSetLSN(ptr->page, recptr);

    } else {
        // Simple case - enough space on page
        START_CRIT_SECTION();

        // Replace or append tuples
        if (OffsetNumberIsValid(oldoffnum)) {
            if (ntup == 1) {
                // One-for-one replacement
                PageIndexTupleOverwrite(page, oldoffnum, (Item) *itup,
                                      IndexTupleSize(*itup));
            } else {
                // Delete old, add new
                PageIndexTupleDelete(page, oldoffnum);
                gistfillbuffer(page, itup, ntup, InvalidOffsetNumber);
            }
        } else {
            // Just append new tuples
            gistfillbuffer(page, itup, ntup, InvalidOffsetNumber);
        }

        MarkBufferDirty(buffer);

        // Log update operation
        if (is_build)
            recptr = GistBuildLSN;
        else if (RelationNeedsWAL(rel))
            recptr = gistXLogUpdate(buffer, /* args */);
        else
            recptr = gistGetFakeLSN(rel);

        PageSetLSN(page, recptr);
        if (newblkno)
            *newblkno = blkno;
    }

    // Update left child page if provided
    if (BufferIsValid(leftchildbuf)) {
        Page leftpg = BufferGetPage(leftchildbuf);
        GistPageSetNSN(leftpg, recptr);
        GistClearFollowRight(leftpg);
        PageSetLSN(leftpg, recptr);
    }

    END_CRIT_SECTION();
    return is_split;
}
```