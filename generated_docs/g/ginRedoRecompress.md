# ginRedoRecompress

## Location
[src/backend/access/gin/ginxlog.c:117-318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginxlog.c#L117-L318)

## Overview
Replays the recompression of posting lists in a GIN data leaf page during WAL recovery, handling complex in-place modifications and format conversions.

## Definition
```c
static void ginRedoRecompress(Page page, ginxlogRecompressDataLeaf *data)
```

## Detailed Description
This function is one of the most complex components in GIN WAL replay, responsible for reconstructing posting list modifications on data leaf pages. It handles multiple types of operations (insert, delete, replace, additems) on compressed posting list segments. The function can also convert pages from pre-9.4 uncompressed format to the modern compressed format.

The function processes a series of actions from the WAL record, each specifying operations on particular segments. To handle space constraints and avoid complex in-place movements, it employs a copy-on-write strategy: once modifications begin, the unprocessed tail of the page is copied to a separate memory area for reference while reconstructing the modified page.

Key operations include:
- Converting legacy uncompressed pages to compressed format
- Processing segment deletions, insertions, replacements, and item additions
- Managing memory efficiently through tail copying
- Ensuring proper segment alignment and size validation

## Parameters / Member Variables
- `page`: The GIN data leaf page to be modified during replay
- `data`: Pointer to ginxlogRecompressDataLeaf structure containing the series of recompression actions

## Dependencies
- Functions called/Symbols referenced:
  - GinPageIsCompressed
  - GinDataPageGetData
  - GinPageGetOpaque
  - [ginCompressPostingList](ginCompressPostingList.md)
  - GinDataLeafPageGetPostingList
  - GinDataPageSetDataSize
  - GinPageSetCompressed
  - GinDataLeafPageGetPostingListSize
  - GinNextPostingListSegment
  - [ginPostingListDecode](ginPostingListDecode.md)
  - [ginMergeItemPointers](ginMergeItemPointers.md)
  - [PageGetSpecialPointer](../P/PageGetSpecialPointer.md)
  - SizeOfGinPostingList
  - SHORTALIGN
  - [palloc](../p/palloc.md)
  - memcpy
  - elog
- Data structures used:
  - ginxlogRecompressDataLeaf
  - [GinPostingList](../G/GinPostingList.md)
  - [ItemPointerData](../I/ItemPointerData.md)
- Constants used:
  - GIN_SEGMENT_DELETE
  - GIN_SEGMENT_INSERT
  - GIN_SEGMENT_REPLACE
  - GIN_SEGMENT_ADDITEMS
  - InvalidOffsetNumber
  - BLCKSZ
- Called from:
  - [ginRedoInsertData](ginRedoInsertData.md)
  - [ginRedoVacuumDataLeafPage](ginRedoVacuumDataLeafPage.md)

## Notes and Other Information
- This is a static function used exclusively within GIN WAL replay operations
- The function includes comprehensive backward compatibility handling for pre-9.4 page formats
- The copy-on-write strategy prevents complex in-place data movement while ensuring correctness
- Memory management includes proper cleanup of temporary allocations
- Error handling includes validation of segment operations and memory bounds
- The function handles empty leaf pages that may exist from pg_upgrade scenarios
- All operations maintain proper posting list compression and alignment requirements

## Simplified Source

```c
static void ginRedoRecompress(Page page, ginxlogRecompressDataLeaf *data)
{
    int actionno, segno;
    GinPostingList *oldseg;
    Pointer segmentend, tailCopy = NULL, writePtr, segptr;
    char *walbuf;
    int totalsize;

    // Convert pre-9.4 uncompressed format to compressed format
    if (!GinPageIsCompressed(page)) {
        ItemPointer uncompressed = (ItemPointer) GinDataPageGetData(page);
        int nuncompressed = GinPageGetOpaque(page)->maxoff;

        if (nuncompressed > 0) {
            int npacked;
            GinPostingList *plist = ginCompressPostingList(uncompressed, nuncompressed, BLCKSZ, &npacked);
            totalsize = SizeOfGinPostingList(plist);
            memcpy(GinDataLeafPageGetPostingList(page), plist, totalsize);
        } else {
            totalsize = 0;
        }

        GinDataPageSetDataSize(page, totalsize);
        GinPageSetCompressed(page);
        GinPageGetOpaque(page)->maxoff = InvalidOffsetNumber;
    }

    // Initialize segment processing
    oldseg = GinDataLeafPageGetPostingList(page);
    writePtr = (Pointer) oldseg;
    segmentend = (Pointer) oldseg + GinDataLeafPageGetPostingListSize(page);
    segno = 0;
    walbuf = ((char *) data) + sizeof(ginxlogRecompressDataLeaf);

    // Process each action from WAL record
    for (actionno = 0; actionno < data->nactions; actionno++) {
        uint8 a_segno = *((uint8 *) (walbuf++));
        uint8 a_action = *((uint8 *) (walbuf++));
        GinPostingList *newseg = NULL;
        int newsegsize = 0;

        // Extract action-specific data from WAL
        if (a_action == GIN_SEGMENT_INSERT || a_action == GIN_SEGMENT_REPLACE) {
            newseg = (GinPostingList *) walbuf;
            newsegsize = SizeOfGinPostingList(newseg);
            walbuf += SHORTALIGN(newsegsize);
        }

        if (a_action == GIN_SEGMENT_ADDITEMS) {
            uint16 nitems;
            memcpy(&nitems, walbuf, sizeof(uint16));
            walbuf += sizeof(uint16);
            ItemPointerData *items = (ItemPointerData *) walbuf;
            walbuf += nitems * sizeof(ItemPointerData);

            // Merge new items with existing segment
            int nolditems, nnewitems, npacked;
            ItemPointerData *olditems = ginPostingListDecode(oldseg, &nolditems);
            ItemPointerData *newitems = ginMergeItemPointers(items, nitems, olditems, nolditems, &nnewitems);
            newseg = ginCompressPostingList(newitems, nnewitems, BLCKSZ, &npacked);
            newsegsize = SizeOfGinPostingList(newseg);
            a_action = GIN_SEGMENT_REPLACE;
        }

        // Skip to target segment
        while (segno < a_segno) {
            int segsize = SizeOfGinPostingList(oldseg);
            if (tailCopy) {
                memcpy(writePtr, (Pointer) oldseg, segsize);
            }
            writePtr += segsize;
            oldseg = GinNextPostingListSegment(oldseg);
            segno++;
        }

        // Copy page tail if modification is starting
        segptr = (Pointer) oldseg;
        if (!tailCopy && segptr != segmentend) {
            int tailSize = segmentend - segptr;
            tailCopy = (Pointer) palloc(tailSize);
            memcpy(tailCopy, segptr, tailSize);
            segptr = tailCopy;
            oldseg = (GinPostingList *) segptr;
            segmentend = segptr + tailSize;
        }

        // Execute the action
        switch (a_action) {
            case GIN_SEGMENT_DELETE:
                // Skip the old segment
                segptr += SizeOfGinPostingList(oldseg);
                segno++;
                break;

            case GIN_SEGMENT_INSERT:
                // Copy new segment
                memcpy(writePtr, newseg, newsegsize);
                writePtr += newsegsize;
                break;

            case GIN_SEGMENT_REPLACE:
                // Replace old segment with new
                memcpy(writePtr, newseg, newsegsize);
                writePtr += newsegsize;
                segptr += SizeOfGinPostingList(oldseg);
                segno++;
                break;
        }
        oldseg = (GinPostingList *) segptr;
    }

    // Copy remaining unmodified segments
    segptr = (Pointer) oldseg;
    if (segptr != segmentend && tailCopy) {
        int restSize = segmentend - segptr;
        memcpy(writePtr, segptr, restSize);
        writePtr += restSize;
    }

    // Update page data size
    totalsize = writePtr - (Pointer) GinDataLeafPageGetPostingList(page);
    GinDataPageSetDataSize(page, totalsize);
}
```