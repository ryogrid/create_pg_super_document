# ginVacuumPostingTreeLeaf

## Location
[src/backend/access/gin/gindatapage.c:738-871](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L738-L871)

## Overview
ginVacuumPostingTreeLeaf performs vacuum operations on a GIN posting tree leaf page, removing dead tuple references and recompressing the page structure while maintaining transactional safety through WAL logging.

## Definition

```c
struct the page from the pieces.
	 *
	 * We don't try to re-encode the segments here, even though some of them
	 * might be really small now that we've removed some items from them. It
	 * seems like a waste of effort, as there isn't really any benefit from
	 * larger segments per se;
```
## Detailed Description
This function implements the vacuum process for GIN (Generalized Inverted Index) posting tree leaf pages. It systematically processes each segment within the leaf page, identifying and removing dead item pointers based on the vacuum state information.

The function operates by first disassembling the leaf page into manageable segments, then iterating through each segment to vacuum individual item pointers. For each segment, it decodes the compressed posting list, calls ginVacuumItemPointers to identify dead tuples, and recompresses the cleaned data. Segments that become empty after vacuuming are marked for deletion.

After processing all segments, if any modifications were made, the function reconstructs the entire page with the cleaned data. The process includes proper WAL logging for crash recovery and operates within critical sections to ensure atomicity. The function also handles memory management by ensuring all modified segments have palloc'd copies as required by the recompression process.

## Parameters / Member Variables
- : The GIN index relation being vacuumed
- : Buffer containing the posting tree leaf page to be vacuumed
- : GinVacuumState structure containing vacuum context and dead tuple information

## Dependencies
- Functions called/Symbols referenced:
  - [disassembleLeaf](../d/disassembleLeaf.md)
  - [ginPostingListDecode](ginPostingListDecode.md)
  - [ginVacuumItemPointers](ginVacuumItemPointers.md)
  - [ginCompressPostingList](ginCompressPostingList.md)
  - [computeLeafRecompressWALData](../c/computeLeafRecompressWALData.md)
  - [dataPlaceToPageLeafRecompress](../d/dataPlaceToPageLeafRecompress.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - RelationNeedsWAL
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - START_CRIT_SECTION/END_CRIT_SECTION
  - dlist_foreach
  - dlist_container
  - [BufferGetPage](../B/BufferGetPage.md)
  - [pfree](../p/pfree.md)
  - [palloc](../p/palloc.md)
  - memcpy
- Called from (representative examples):
  - [ginVacuumPostingTreeLeaves](ginVacuumPostingTreeLeaves.md)

## Notes and Other Information
- The function preserves the original segment structure rather than re-encoding for optimal packing, deferring optimization to future insertions
- Pages in pre-9.4 uncompressed format are treated as single large segments without further splitting
- Memory management ensures palloc'd copies of all segments after the first modified segment
- WAL logging uses XLOG_GIN_VACUUM_DATA_LEAF_PAGE record type
- Critical sections ensure atomicity of page modifications
- Empty segments after vacuuming are marked for deletion rather than kept as zero-length segments
- The function handles both compressed and uncompressed posting list formats

## Simplified Source

```c
void
ginVacuumPostingTreeLeaf(Relation indexrel, Buffer buffer, GinVacuumState *gvs)
{
    Page page = BufferGetPage(buffer);
    disassembledLeaf *leaf;
    bool removedsomething = false;
    dlist_iter iter;

    // Break page into manageable segments
    leaf = disassembleLeaf(page);

    // Process each segment to remove dead tuples
    dlist_foreach(iter, &leaf->segments)
    {
        leafSegmentInfo *seginfo = dlist_container(leafSegmentInfo, node, iter.cur);
        ItemPointer cleaned;
        int ncleaned;

        // Decode posting list if needed
        if (!seginfo->items)
            seginfo->items = ginPostingListDecode(seginfo->seg, &seginfo->nitems);

        // Remove dead tuples from this segment
        cleaned = ginVacuumItemPointers(gvs, seginfo->items, seginfo->nitems, &ncleaned);

        pfree(seginfo->items);
        seginfo->items = NULL;

        if (cleaned)
        {
            if (ncleaned > 0)
            {
                // Recompress cleaned data
                seginfo->seg = ginCompressPostingList(cleaned, ncleaned,
                                                     SizeOfGinPostingList(seginfo->seg), &npacked);
                seginfo->action = GIN_SEGMENT_REPLACE;
            }
            else
            {
                // Mark empty segment for deletion
                seginfo->seg = NULL;
                seginfo->action = GIN_SEGMENT_DELETE;
            }
            seginfo->nitems = ncleaned;
            removedsomething = true;
        }
    }

    // Rebuild page if anything was removed
    if (removedsomething)
    {
        // Prepare WAL data and apply changes
        if (RelationNeedsWAL(indexrel))
            computeLeafRecompressWALData(leaf);

        START_CRIT_SECTION();

        dataPlaceToPageLeafRecompress(buffer, leaf);
        MarkBufferDirty(buffer);

        // Log the vacuum operation
        if (RelationNeedsWAL(indexrel))
        {
            XLogRecPtr recptr;
            XLogBeginInsert();
            XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
            XLogRegisterBufData(0, leaf->walinfo, leaf->walinfolen);
            recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_VACUUM_DATA_LEAF_PAGE);
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();
    }
}
```