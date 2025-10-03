# dataPlaceToPageLeafRecompress

## Location
[src/backend/access/gin/gindatapage.c:978-1033](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L978-L1033)

## Overview
dataPlaceToPageLeafRecompress reconstructs a GIN data leaf page from a disassembled representation, applying all segment modifications and handling format conversions from pre-9.4 uncompressed format.

## Definition

```c
static void
dataPlaceToPageLeafRecompress(Buffer buf, disassembledLeaf *leaf)
```
## Detailed Description
This function reassembles a GIN data leaf page by applying all modifications stored in a disassembledLeaf structure to the target buffer. It handles the physical reconstruction of the page content by iterating through all segments and copying modified data to the appropriate locations.

The function includes special handling for format conversion from PostgreSQL pre-9.4 uncompressed format to the modern compressed format. When encountering an uncompressed page, it converts the page header and forces all segments to be rewritten regardless of modification status.

During reassembly, the function tracks whether any modifications have been made and only performs memory copies for segments that need updating or that follow modified segments. Deleted segments are skipped entirely, while other segments have their compressed posting list data copied to the target location. The function concludes by updating the page's data size field with the total size of all assembled segments.

An important constraint is that segment pointers must not point directly to the same buffer being modified, except for unmodified segments whose preceding segments are also unmodified. This ensures memory safety during the reconstruction process.

## Parameters / Member Variables
- : Target buffer containing the leaf page to be reconstructed
- : Pointer to disassembledLeaf structure containing segment modifications and data

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - GinPageIsCompressed
  - GinPageSetCompressed
  - GinPageGetOpaque
  - GinDataLeafPageGetPostingList
  - GinDataPageSetDataSize
  - SizeOfGinPostingList
  - dlist_foreach
  - dlist_container
  - memcpy
  - Assert
  - leafSegmentInfo (structure type)
  - [dlist_iter](dlist_iter.md) (structure type)
  - InvalidOffsetNumber (constant)
  - GIN_SEGMENT_UNMODIFIED (constant)
  - GIN_SEGMENT_DELETE (constant)
  - GinDataPageMaxDataSize (constant)
- Called from (representative examples):
  - [dataExecPlaceToPageLeaf](dataExecPlaceToPageLeaf.md)
  - [ginVacuumPostingTreeLeaf](../g/ginVacuumPostingTreeLeaf.md)

## Notes and Other Information
- Function only updates the target buffer; WAL logging is the caller's responsibility
- Segment pointers must not reference the same buffer being modified (with specific exceptions)
- Automatically converts pre-9.4 uncompressed page format to compressed format
- Only copies segment data when modifications are detected or when following modified segments
- Deleted segments contribute zero bytes to the final page size
- Final page size is validated against GinDataPageMaxDataSize limit
- Function assumes all necessary memory allocations and segment preparations have been completed
- Format conversion sets maxoff to InvalidOffsetNumber and enables compression flag

## Simplified Source

```c
static void
dataPlaceToPageLeafRecompress(Buffer buf, disassembledLeaf *leaf)
{
    Page page = BufferGetPage(buf);
    char *data_ptr;
    int total_size;
    bool modified = false;
    dlist_iter iter;

    // Convert old uncompressed format to compressed format if needed
    if (!GinPageIsCompressed(page))
    {
        GinPageSetCompressed(page);
        GinPageGetOpaque(page)->maxoff = InvalidOffsetNumber;
        modified = true;
    }

    // Build the new page content
    data_ptr = (char *) GinDataLeafPageGetPostingList(page);
    total_size = 0;

    dlist_foreach(iter, &leaf->segments)
    {
        leafSegmentInfo *seginfo = dlist_container(leafSegmentInfo, node, iter.cur);

        // Track if any modifications occurred
        if (seginfo->action != GIN_SEGMENT_UNMODIFIED)
            modified = true;

        // Copy non-deleted segments to page
        if (seginfo->action != GIN_SEGMENT_DELETE)
        {
            int seg_size = SizeOfGinPostingList(seginfo->seg);

            // Only copy if modifications occurred
            if (modified)
                memcpy(data_ptr, seginfo->seg, seg_size);

            data_ptr += seg_size;
            total_size += seg_size;
        }
    }

    // Update page size
    Assert(total_size <= GinDataPageMaxDataSize);
    GinDataPageSetDataSize(page, total_size);
}
```