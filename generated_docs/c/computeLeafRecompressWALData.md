# computeLeafRecompressWALData

## Location
[src/backend/access/gin/gindatapage.c:872-977](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L872-L977)

## Overview
computeLeafRecompressWALData constructs WAL record data for GIN leaf page recompression operations, encoding segment modifications into a format suitable for crash recovery replay.

## Definition

```c
structed info via *leaf */
	leaf->walinfo = walbufbegin;
```
## Detailed Description
This function prepares Write-Ahead Logging (WAL) data for GIN data leaf page recompression operations by analyzing a disassembledLeaf structure and encoding the changes into a ginxlogRecompressDataLeaf record format. The function must be called before entering the critical section that performs the actual page updates because it requires memory allocation.

The function first counts all modified segments, then allocates a buffer large enough to hold the WAL record header plus all segment data. It iterates through each segment, recording the segment number and action type, followed by the appropriate data based on the action. For efficiency, it can optimize ADDITEMS actions by converting them to REPLACE actions when the compressed segment data is smaller than the uncompressed item pointer list.

The resulting WAL data contains a complete description of all changes needed to reconstruct the leaf page modifications during recovery, including deletions, insertions, replacements, and item additions. The function stores the constructed WAL buffer and its length in the disassembledLeaf structure for later use during WAL record creation.

## Parameters / Member Variables
- : Pointer to disassembledLeaf structure containing segment modification information; function updates leaf->walinfo and leaf->walinfolen fields

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach
  - dlist_container
  - [palloc](../p/palloc.md)
  - memcpy
  - elog
  - SizeOfGinPostingList
  - SHORTALIGN
  - leafSegmentInfo (structure type)
  - ginxlogRecompressDataLeaf (structure type)
  - [dlist_iter](../d/dlist_iter.md) (structure type)
  - GIN_SEGMENT_UNMODIFIED (constant)
  - GIN_SEGMENT_DELETE (constant)
  - GIN_SEGMENT_ADDITEMS (constant)
  - GIN_SEGMENT_INSERT (constant)
  - GIN_SEGMENT_REPLACE (constant)
  - [ItemPointerData](../I/ItemPointerData.md) (structure type)
- Called from (representative examples):
  - [dataBeginPlaceToPageLeaf](../d/dataBeginPlaceToPageLeaf.md)
  - [ginVacuumPostingTreeLeaf](../g/ginVacuumPostingTreeLeaf.md)

## Notes and Other Information
- Must be called before entering critical sections due to palloc usage
- Automatically optimizes ADDITEMS to REPLACE when compressed data is smaller
- Allocates maximum possible buffer size (BLCKSZ + overhead) to avoid complex size calculations
- WAL data format includes action count, followed by (segment_number, action, data) tuples
- Different actions store different data formats: DELETE stores nothing, ADDITEMS stores item count and items, INSERT/REPLACE store compressed segment data
- Uses SHORTALIGN for segment data alignment in WAL records
- Segment numbering excludes inserted segments until after processing
- The constructed WAL data enables complete page reconstruction during recovery replay

## Simplified Source

```c
static void
computeLeafRecompressWALData(disassembledLeaf *leaf)
{
    int nmodified = 0;
    char *walbuf_start, *walbuf_ptr;
    dlist_iter iter;
    int segno;
    ginxlogRecompressDataLeaf *recompress_xlog;

    // Count modified segments
    dlist_foreach(iter, &leaf->segments)
    {
        leafSegmentInfo *seginfo = dlist_container(leafSegmentInfo, node, iter.cur);
        if (seginfo->action != GIN_SEGMENT_UNMODIFIED)
            nmodified++;
    }

    // Allocate WAL buffer (generous size to avoid complex calculations)
    walbuf_start = palloc(sizeof(ginxlogRecompressDataLeaf) + BLCKSZ + nmodified * 2);
    walbuf_ptr = walbuf_start;

    // Write WAL record header
    recompress_xlog = (ginxlogRecompressDataLeaf *) walbuf_ptr;
    walbuf_ptr += sizeof(ginxlogRecompressDataLeaf);
    recompress_xlog->nactions = nmodified;

    // Process each modified segment
    segno = 0;
    dlist_foreach(iter, &leaf->segments)
    {
        leafSegmentInfo *seginfo = dlist_container(leafSegmentInfo, node, iter.cur);
        uint8 action = seginfo->action;
        int datalen;

        if (action == GIN_SEGMENT_UNMODIFIED)
        {
            segno++;
            continue;
        }

        // Optimize: use REPLACE instead of ADDITEMS if compressed data is smaller
        if (action == GIN_SEGMENT_ADDITEMS &&
            seginfo->nmodifieditems * sizeof(ItemPointerData) > SizeOfGinPostingList(seginfo->seg))
        {
            action = GIN_SEGMENT_REPLACE;
        }

        // Write segment number and action
        *((uint8 *) (walbuf_ptr++)) = segno;
        *(walbuf_ptr++) = action;

        // Write action-specific data
        switch (action)
        {
            case GIN_SEGMENT_DELETE:
                datalen = 0;
                break;

            case GIN_SEGMENT_ADDITEMS:
                datalen = seginfo->nmodifieditems * sizeof(ItemPointerData);
                memcpy(walbuf_ptr, &seginfo->nmodifieditems, sizeof(uint16));
                memcpy(walbuf_ptr + sizeof(uint16), seginfo->modifieditems, datalen);
                datalen += sizeof(uint16);
                break;

            case GIN_SEGMENT_INSERT:
            case GIN_SEGMENT_REPLACE:
                datalen = SHORTALIGN(SizeOfGinPostingList(seginfo->seg));
                memcpy(walbuf_ptr, seginfo->seg, SizeOfGinPostingList(seginfo->seg));
                break;

            default:
                elog(ERROR, "unexpected GIN leaf action %d", action);
        }

        walbuf_ptr += datalen;

        if (action != GIN_SEGMENT_INSERT)
            segno++;
    }

    // Store WAL data in leaf structure
    leaf->walinfo = walbuf_start;
    leaf->walinfolen = walbuf_ptr - walbuf_start;
}
```