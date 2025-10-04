# SpGistGetBuffer

## Location
[src/backend/access/spgist/spgutils.c:561-664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L561-L664)

## Overview
Retrieves a buffer page for SP-GiST index operations with specified type, parity, and minimum free space requirements, utilizing a cache-first strategy for efficiency.

## Definition
Buffer SpGistGetBuffer(Relation index, int flags, int needSpace, bool *isNew)

## Detailed Description
This function implements an intelligent buffer allocation strategy for SP-GiST indexes by first checking the lastUsedPages cache before allocating new pages. It validates that the requested space requirements can be satisfied (even on an empty page), then applies the index's fillfactor to the space request to maintain optimal page utilization.

The function employs a three-tier approach: first checking cache for suitable existing pages, then attempting to reuse cached pages even if they need reinitialization, and finally falling back to allocating completely new pages. When examining cached pages, it performs comprehensive validation including lock availability, page state, type compatibility, null-handling requirements, and actual free space.

The function sets the isNew output parameter to indicate whether the returned page was newly initialized, helping callers understand the page's state and optimize their subsequent operations.

## Parameters / Member Variables
- : Relation object representing the SP-GiST index requiring a buffer page
- : Bit flags specifying page type (leaf/inner), null handling, and parity requirements
- : Minimum required free space in bytes that the returned page must provide
- : Output parameter set to true if the page was initialized during this call, false if it was already valid

## Dependencies
- Functions called/Symbols referenced:
  - [spgGetCache](../s/spgGetCache.md)
  - SpGistGetTargetPageFreeSpace
  - GET_LUP
  - [allocNewBuffer](../a/allocNewBuffer.md)
  - SpGistBlockIsFixed
  - [ReadBuffer](../R/ReadBuffer.md)
  - [ConditionalLockBuffer](../C/ConditionalLockBuffer.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageIsNew](../P/PageIsNew.md)
  - SpGistPageIsDeleted
  - [PageIsEmpty](../P/PageIsEmpty.md)
  - [SpGistInitBuffer](SpGistInitBuffer.md)
  - [PageGetExactFreeSpace](../P/PageGetExactFreeSpace.md)
  - SpGistPageIsLeaf
  - SpGistPageStoresNulls
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Called from (representative examples):
  - [moveLeafs](../m/moveLeafs.md)
  - [doPickSplit](../d/doPickSplit.md)
  - [spgAddNodeAction](../s/spgAddNodeAction.md)
  - [spgSplitNodeAction](../s/spgSplitNodeAction.md)
  - [spgdoinsert](../s/spgdoinsert.md)

## Notes and Other Information
The function includes error checking to prevent requests for impossible space amounts exceeding SPGIST_PAGE_CAPACITY. It uses ConditionalLockBuffer to avoid blocking on busy pages, preferring to allocate new pages rather than wait. The fillfactor consideration helps maintain index performance by preserving space for related tuples. Cache entries are updated with actual free space measurements after successful allocations to maintain accuracy.

## Simplified Source

```c
Buffer SpGistGetBuffer(Relation index, int flags, int needSpace, bool *isNew) {
    SpGistCache *cache = spgGetCache(index);
    SpGistLastUsedPage *lup;

    // Validate space requirement
    if (needSpace > SPGIST_PAGE_CAPACITY)
        elog(ERROR, "desired SPGiST tuple size is too big");

    // Apply fillfactor to space request
    needSpace += SpGistGetTargetPageFreeSpace(index);
    needSpace = Min(needSpace, SPGIST_PAGE_CAPACITY);

    // Get cache entry for this flag combination
    lup = GET_LUP(cache, flags);

    // No cached page available
    if (lup->blkno == InvalidBlockNumber) {
        *isNew = true;
        return allocNewBuffer(index, flags);
    }

    // Check if cached page has enough free space
    if (lup->freeSpace >= needSpace) {
        Buffer buffer = ReadBuffer(index, lup->blkno);

        // Try to lock buffer (non-blocking)
        if (!ConditionalLockBuffer(buffer)) {
            ReleaseBuffer(buffer);
            *isNew = true;
            return allocNewBuffer(index, flags);
        }

        Page page = BufferGetPage(buffer);

        // Check if page needs initialization
        if (PageIsNew(page) || SpGistPageIsDeleted(page) || PageIsEmpty(page)) {
            uint16 pageflags = 0;
            if (GBUF_REQ_LEAF(flags))
                pageflags |= SPGIST_LEAF;
            if (GBUF_REQ_NULLS(flags))
                pageflags |= SPGIST_NULLS;

            SpGistInitBuffer(buffer, pageflags);
            lup->freeSpace = PageGetExactFreeSpace(page) - needSpace;
            *isNew = true;
            return buffer;
        }

        // Validate page type and space
        if ((GBUF_REQ_LEAF(flags) ? SpGistPageIsLeaf(page) : !SpGistPageIsLeaf(page)) &&
            (GBUF_REQ_NULLS(flags) ? SpGistPageStoresNulls(page) : !SpGistPageStoresNulls(page))) {

            int freeSpace = PageGetExactFreeSpace(page);
            if (freeSpace >= needSpace) {
                lup->freeSpace = freeSpace - needSpace;
                *isNew = false;
                return buffer;
            }
        }

        UnlockReleaseBuffer(buffer);
    }

    // Fallback to new buffer allocation
    *isNew = true;
    return allocNewBuffer(index, flags);
}
```