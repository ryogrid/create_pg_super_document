# leafRepackItems

## Location
[src/backend/access/gin/gindatapage.c:1571-1774](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L1571-L1774)

## Overview
leafRepackItems recompresses all modified segments in a disassembled GIN data leaf page and determines if the page needs to be split due to size constraints.

## Definition

```c
structed did not
				 * fit.
				 */
				*remaining = seginfo->seg->first;
```
## Detailed Description
This complex static function is responsible for the final stage of leaf page modification in GIN indexes. It processes all segments in a disassembledLeaf structure and performs several critical operations:

1. **Compression**: Compresses modified segments using ginCompressPostingList, attempting to fit them within size limits
2. **Segment Splitting**: When segments are too large, splits them into smaller segments that fit within the target size
3. **Segment Merging**: Merges very small segments with adjacent segments to maintain efficient storage
4. **Size Management**: Tracks total page usage and determines if the page needs to be split across two pages
5. **Memory Safety**: Creates palloc'd copies of segments that might be overwritten during page reconstruction

The function implements sophisticated logic to balance storage efficiency with page size constraints. It handles both left and right pages during splits and sets the remaining parameter to indicate items that didn't fit if a split is necessary.

Returns true if the page must be split into two pages, false if all items fit on a single page.

## Parameters / Member Variables
- : The disassembledLeaf structure containing segments to repack
- : Output parameter set to the first ItemPointer that didn't fit if splitting is required

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - [dlist_head_node](../d/dlist_head_node.md)
  - dlist_container
  - [dlist_has_next](../d/dlist_has_next.md)
  - [dlist_next_node](../d/dlist_next_node.md)
  - [ginCompressPostingList](../g/ginCompressPostingList.md)
  - [dlist_insert_after](../d/dlist_insert_after.md)
  - [dlist_delete](../d/dlist_delete.md)
  - [dlist_prev_node](../d/dlist_prev_node.md)
  - SizeOfGinPostingList
  - [ginPostingListDecode](../g/ginPostingListDecode.md)
  - [ginMergeItemPointers](../g/ginMergeItemPointers.md)
  - dlist_foreach
  - [palloc](../p/palloc.md)
  - [pfree](../p/pfree.md)
  - memcpy
  - GIN_SEGMENT_DELETE
  - GIN_SEGMENT_INSERT
  - GIN_SEGMENT_REPLACE
  - GIN_SEGMENT_UNMODIFIED
  - GinPostingListSegmentMaxSize
  - GinPostingListSegmentTargetSize
  - GinPostingListSegmentMinSize
  - GinDataPageMaxDataSize
- Called from (representative examples):
  - [dataBeginPlaceToPageLeaf](../d/dataBeginPlaceToPageLeaf.md)

## Notes and Other Information
- This is a static function, only accessible within gindatapage.c
- Implements sophisticated segment management including splitting oversized segments and merging undersized ones
- Handles memory management carefully to avoid overwriting existing segments during page reconstruction
- Tracks page usage precisely to determine split points and remaining items
- The function modifies the disassembledLeaf structure extensively, updating segment lists and size information
- Uses custom iteration logic instead of dlist_foreach_modify due to the complex segment insertions during iteration
- Located in src/backend/access/gin/gindatapage.c at lines 1571-1774
- Critical component of GIN index page split and reorganization operations

## Simplified Source

```c
static bool
leafRepackItems(disassembledLeaf *leaf, ItemPointer remaining)
{
    int pgused = 0;
    bool needsplit = false;
    dlist_node *cur_node, *next_node;

    ItemPointerSetInvalid(remaining);

    // Process each segment, handling compression and splitting
    for (cur_node = dlist_head_node(&leaf->segments); cur_node != NULL; cur_node = next_node) {
        leafSegmentInfo *seginfo = dlist_container(leafSegmentInfo, node, cur_node);

        next_node = dlist_has_next(&leaf->segments, cur_node) ?
                   dlist_next_node(&leaf->segments, cur_node) : NULL;

        // Skip deleted segments
        if (seginfo->action == GIN_SEGMENT_DELETE)
            continue;

        // Compress segment if needed
        if (seginfo->seg == NULL) {
            int npacked;

            if (seginfo->nitems > GinPostingListSegmentMaxSize) {
                npacked = 0;  // Too large to fit
            } else {
                seginfo->seg = ginCompressPostingList(seginfo->items, seginfo->nitems,
                                                     GinPostingListSegmentMaxSize, &npacked);
            }

            // If segment too large, split it
            if (npacked != seginfo->nitems) {
                if (seginfo->seg) pfree(seginfo->seg);

                seginfo->seg = ginCompressPostingList(seginfo->items, seginfo->nitems,
                                                     GinPostingListSegmentTargetSize, &npacked);
                if (seginfo->action != GIN_SEGMENT_INSERT)
                    seginfo->action = GIN_SEGMENT_REPLACE;

                // Create new segment for remaining items
                leafSegmentInfo *nextseg = palloc(sizeof(leafSegmentInfo));
                nextseg->action = GIN_SEGMENT_INSERT;
                nextseg->seg = NULL;
                nextseg->items = &seginfo->items[npacked];
                nextseg->nitems = seginfo->nitems - npacked;
                next_node = &nextseg->node;
                dlist_insert_after(cur_node, next_node);
            }
        }

        // Merge small segments with next segment
        if (SizeOfGinPostingList(seginfo->seg) < GinPostingListSegmentMinSize && next_node) {
            leafSegmentInfo *nextseg = dlist_container(leafSegmentInfo, node, next_node);

            // Decode both segments if needed
            if (!seginfo->items)
                seginfo->items = ginPostingListDecode(seginfo->seg, &seginfo->nitems);
            if (!nextseg->items)
                nextseg->items = ginPostingListDecode(nextseg->seg, &nextseg->nitems);

            // Merge the segments
            int nmerged;
            nextseg->items = ginMergeItemPointers(seginfo->items, seginfo->nitems,
                                                 nextseg->items, nextseg->nitems, &nmerged);
            nextseg->nitems = nmerged;
            nextseg->seg = NULL;
            nextseg->action = GIN_SEGMENT_REPLACE;

            // Handle current segment deletion
            if (seginfo->action == GIN_SEGMENT_INSERT) {
                dlist_delete(cur_node);
                continue;
            } else {
                seginfo->action = GIN_SEGMENT_DELETE;
                seginfo->seg = NULL;
                continue;
            }
        }

        // Check if segment fits on current page
        int segsize = SizeOfGinPostingList(seginfo->seg);
        if (pgused + segsize > GinDataPageMaxDataSize) {
            if (!needsplit) {
                // Switch to right page
                leaf->lastleft = dlist_prev_node(&leaf->segments, cur_node);
                needsplit = true;
                leaf->lsize = pgused;
                pgused = 0;
            } else {
                // Both pages full - set remaining items
                *remaining = seginfo->seg->first;

                // Remove segments that don't fit
                while (dlist_has_next(&leaf->segments, cur_node))
                    dlist_delete(dlist_next_node(&leaf->segments, cur_node));
                dlist_delete(cur_node);
                break;
            }
        }

        pgused += segsize;
        seginfo->items = NULL;
        seginfo->nitems = 0;
    }

    // Set final sizes
    if (!needsplit) {
        leaf->lsize = pgused;
        leaf->rsize = 0;
    } else {
        leaf->rsize = pgused;
    }

    // Create palloc'd copies of unmodified segments that come after modified ones
    bool modified = false;
    dlist_iter iter;
    dlist_foreach(iter, &leaf->segments) {
        leafSegmentInfo *seginfo = dlist_container(leafSegmentInfo, node, iter.cur);

        if (!modified && seginfo->action != GIN_SEGMENT_UNMODIFIED) {
            modified = true;
        } else if (modified && seginfo->action == GIN_SEGMENT_UNMODIFIED) {
            int segsize = SizeOfGinPostingList(seginfo->seg);
            GinPostingList *tmp = palloc(segsize);
            memcpy(tmp, seginfo->seg, segsize);
            seginfo->seg = tmp;
        }
    }

    return needsplit;
}
```