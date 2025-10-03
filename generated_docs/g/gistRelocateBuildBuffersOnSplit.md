# gistRelocateBuildBuffersOnSplit

## Location
[src/backend/access/gist/gistbuildbuffers.c:533-749](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuildbuffers.c#L533-L749)

## Overview
Redistributes buffered tuples from a split page to the appropriate new buffer pages during GiST index construction, using penalty-based page selection.

## Definition
```c
void gistRelocateBuildBuffersOnSplit(GISTBuildBuffers *gfbb, GISTSTATE *giststate,
                                   Relation r, int level, Buffer buffer, List *splitinfo)
```

## Detailed Description
This complex function handles one of the most critical aspects of GiST index construction: redistributing buffered tuples when a page splits. When a node splits during index construction, any tuples buffered for that node must be redistributed to the appropriate new nodes created by the split.

The function implements a sophisticated tuple placement algorithm similar to gistchoose(), computing penalties for each possible target page and selecting the one with the minimum penalty. It processes each tuple by examining all index key attributes and finding the page that results in the lowest penalty, with ties broken by examining subsequent attributes.

The function also handles the complex memory management involved in buffer relocation, creating temporary copies of the original buffer and properly initializing new buffers for the split pages.

## Parameters / Member Variables
- `gfbb`: Pointer to the GiST build buffers structure containing global build state
- `giststate`: GiST state information for the index being built
- `r`: The relation (index) being constructed
- `level`: The tree level at which the split is occurring
- `buffer`: The buffer containing the page that was split
- `splitinfo`: List of information about the pages created by the split

## Dependencies
- Functions called/Symbols referenced:
  - LEVEL_HAS_BUFFERS
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [hash_search](../h/hash_search.md)
  - [gistDeCompressAtt](gistDeCompressAtt.md)
  - [gistGetNodeBuffer](gistGetNodeBuffer.md)
  - [gistPopItupFromNodeBuffer](gistPopItupFromNodeBuffer.md)
  - [gistpenalty](gistpenalty.md)
  - [gistPushItupToNodeBuffer](gistPushItupToNodeBuffer.md)
  - [gistgetadjusted](gistgetadjusted.md)
  - IndexRelationGetNumberOfKeyAttributes
- Called from (representative examples):
  - [gistbufferinginserttuples](gistbufferinginserttuples.md)

## Notes and Other Information
- Returns early if the level doesn't use buffers or if no buffer exists for the split page
- Uses a penalty-based algorithm identical to the page selection logic in gistchoose()
- Handles the complex case where the leftmost split page reuses the original buffer
- Updates downlink tuples when necessary to maintain index consistency
- Critical for maintaining buffer organization during dynamic page splits
- Implements sophisticated memory management to handle buffer relocation safely

## Simplified Source

```c
void gistRelocateBuildBuffersOnSplit(GISTBuildBuffers *gfbb, GISTSTATE *giststate,
                                   Relation r, int level, Buffer buffer, List *splitinfo) {
    // Early exit if level doesn't use buffers
    if (!LEVEL_HAS_BUFFERS(level, gfbb))
        return;

    // Find the node buffer for the split page
    BlockNumber blocknum = BufferGetBlockNumber(buffer);
    GISTNodeBuffer *nodeBuffer = hash_search(gfbb->nodeBuffersTab, &blocknum, HASH_FIND, &found);
    if (!found)
        return;  // No buffer exists for this page

    // Make a copy of the old buffer and reset the original
    GISTNodeBuffer oldBuf;
    memcpy(&oldBuf, nodeBuffer, sizeof(GISTNodeBuffer));
    oldBuf.isTemp = true;

    nodeBuffer->blocksCount = 0;
    nodeBuffer->pageBuffer = NULL;

    // Prepare relocation info for each split page
    int splitPagesCount = list_length(splitinfo);
    RelocationBufferInfo *relocationBuffersInfos =
        (RelocationBufferInfo *) palloc(sizeof(RelocationBufferInfo) * splitPagesCount);

    // Initialize buffers for each split page
    foreach(lc, splitinfo) {
        GISTPageSplitInfo *si = (GISTPageSplitInfo *) lfirst(lc);
        int i = foreach_current_index(lc);

        // Decompress downlink entry for penalty calculation
        gistDeCompressAtt(giststate, r, si->downlink, NULL, 0,
                         relocationBuffersInfos[i].entry,
                         relocationBuffersInfos[i].isnull);

        // Get node buffer for this split page
        relocationBuffersInfos[i].nodeBuffer =
            gistGetNodeBuffer(gfbb, giststate, BufferGetBlockNumber(si->buf), level);
        relocationBuffersInfos[i].splitinfo = si;
    }

    // Redistribute all tuples from old buffer to new buffers
    IndexTuple itup;
    while (gistPopItupFromNodeBuffer(gfbb, &oldBuf, &itup)) {
        GISTENTRY entry[INDEX_MAX_KEYS];
        bool isnull[INDEX_MAX_KEYS];
        float best_penalty[INDEX_MAX_KEYS];
        int which = 0;  // Default to first page

        // Decompress tuple for penalty calculation
        gistDeCompressAtt(giststate, r, itup, NULL, 0, entry, isnull);
        best_penalty[0] = -1;

        // Find the page with minimum penalty
        for (int i = 0; i < splitPagesCount; i++) {
            RelocationBufferInfo *splitPageInfo = &relocationBuffersInfos[i];
            bool zero_penalty = true;

            // Calculate penalty for each index attribute
            for (int j = 0; j < IndexRelationGetNumberOfKeyAttributes(r); j++) {
                float usize = gistpenalty(giststate, j,
                                        &splitPageInfo->entry[j],
                                        splitPageInfo->isnull[j],
                                        &entry[j], isnull[j]);
                if (usize > 0)
                    zero_penalty = false;

                // Update best penalty and target page
                if (best_penalty[j] < 0 || usize < best_penalty[j]) {
                    which = i;
                    best_penalty[j] = usize;
                    if (j < IndexRelationGetNumberOfKeyAttributes(r) - 1)
                        best_penalty[j + 1] = -1;
                } else if (best_penalty[j] != usize) {
                    break;  // This page is worse, try next page
                }
            }

            if (zero_penalty)
                break;  // Perfect match found
        }

        // Place tuple in selected buffer and update downlink if needed
        RelocationBufferInfo *targetBufferInfo = &relocationBuffersInfos[which];
        gistPushItupToNodeBuffer(gfbb, targetBufferInfo->nodeBuffer, itup);

        IndexTuple newtup = gistgetadjusted(r, targetBufferInfo->splitinfo->downlink,
                                           itup, giststate);
        if (newtup) {
            gistDeCompressAtt(giststate, r, newtup, NULL, 0,
                             targetBufferInfo->entry, targetBufferInfo->isnull);
            targetBufferInfo->splitinfo->downlink = newtup;
        }
    }

    pfree(relocationBuffersInfos);
}
```