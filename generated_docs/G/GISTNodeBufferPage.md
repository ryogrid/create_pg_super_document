# GISTNodeBufferPage

## Location
[src/include/access/gist_private.h:51-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gist_private.h#L51-L52)

## Overview
GISTNodeBufferPage represents the structure of a buffer page used during GiST index construction to temporarily store tuples before they are written to disk pages.

## Definition

```c
char		tupledata[FLEXIBLE_ARRAY_MEMBER];
} GISTNodeBufferPage;

#define BUFFER_PAGE_DATA_OFFSET MAXALIGN(offsetof(GISTNodeBufferPage, tupledata))
/* Returns free space in node buffer page */
#define PAGE_FREE_SPACE(nbp) (nbp->freespace)
/* Checks if node buffer page is empty */
#define PAGE_IS_EMPTY(nbp) (nbp->freespace == BLCKSZ - BUFFER_PAGE_DATA_OFFSET)
/* Checks if node buffers page don't contain sufficient space for index tuple */
#define PAGE_NO_SPACE(nbp, itup) (PAGE_FREE_SPACE(nbp) < \
										MAXALIGN(IndexTupleSize(itup)))

/*
 * GISTSTATE: information needed for any GiST index operation
 *
 * This struct retains call info for the index's opclass-specific support
 * functions (per index column), plus the index's tuple descriptor.
 *
 * scanCxt holds the GISTSTATE itself as well as any data that lives for the
 * lifetime of the index operation.  We pass this to the support functions
 * via fn_mcxt, so that they can store scan-lifespan data in it.  The
 * functions are invoked in tempCxt, which is typically short-lifespan
 * (that is, it's reset after each tuple).  However, tempCxt can be the same
 * as scanCxt if we're not bothering with per-tuple context resets.
 */
typedef struct GISTSTATE
```
## Detailed Description
GISTNodeBufferPage is a data structure used internally during GiST index building to manage temporary storage of index tuples. It serves as a buffer page that can hold multiple tuples before they are flushed to actual disk pages. The structure is designed to efficiently manage space and maintain links between buffer pages through the prev field.

The structure uses a flexible array member for tupledata, allowing it to store variable amounts of tuple data within a single allocated block. The freespace field tracks available space, enabling efficient space management and preventing overflow.

## Parameters / Member Variables
- : BlockNumber pointing to the previous buffer page, forming a linked list of buffer pages
- : Amount of free space remaining in this buffer page for storing additional tuples
- : Flexible array member containing the actual index tuple data stored in this buffer page

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (for prev field)
  - FLEXIBLE_ARRAY_MEMBER (for tupledata array)
- Called from (representative examples):
  - gistAllocateNewPageBuffer (allocates and initializes new buffer pages)
  - gistGetNodeBuffer (retrieves buffer pages)
  - [gistPlaceItupToPage](../g/gistPlaceItupToPage.md) (places tuples into buffer pages)
  - [gistGetItupFromPage](../g/gistGetItupFromPage.md) (retrieves tuples from buffer pages)
  - [gistPushItupToNodeBuffer](../g/gistPushItupToNodeBuffer.md) (adds tuples to node buffers)

## Notes and Other Information
- Buffer pages are allocated with BLCKSZ size to match PostgreSQL's standard block size
- The BUFFER_PAGE_DATA_OFFSET macro calculates the offset where tuple data begins
- Related macros PAGE_FREE_SPACE, PAGE_IS_EMPTY, and PAGE_NO_SPACE provide convenient operations on buffer pages
- Used primarily during GiST index construction phase, not during normal index operations
- Memory is allocated in the build context and automatically freed when construction completes