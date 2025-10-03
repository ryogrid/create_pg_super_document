# disassembleLeaf

## Location
[src/backend/access/gin/gindatapage.c:1370-1443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L1370-L1443)

## Overview
disassembleLeaf disassembles a GIN data leaf page into a structured disassembledLeaf representation that can be manipulated and later reassembled.

## Definition

```c
static disassembledLeaf *
disassembleLeaf(Page page)
```
## Detailed Description
This static function takes a GIN data leaf page and converts it into a disassembledLeaf structure that provides a more convenient representation for manipulation operations. The function handles both compressed (9.4+ format) and uncompressed (pre-9.4 format) page formats differently:

For compressed pages, it creates a leafSegmentInfo entry for each posting list segment on the page, preserving the original segment structure. Each segment is marked as GIN_SEGMENT_UNMODIFIED initially.

For uncompressed pages, it creates a single segment containing all the ItemPointer entries from the page, marked as GIN_SEGMENT_REPLACE since the old format needs to be converted. Empty uncompressed pages result in no segments.

The resulting disassembledLeaf structure uses a doubly-linked list to track all segments, making it easy to insert, modify, or remove segments during page manipulation operations.

## Parameters / Member Variables
- `page`: The GIN data leaf page to be disassembled
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [palloc](../p/palloc.md)
  - [dlist_init](dlist_init.md)
  - [dlist_push_tail](dlist_push_tail.md)
  - GinPageIsCompressed
  - GinDataLeafPageGetPostingList
  - GinDataLeafPageGetPostingListSize
  - GinNextPostingListSegment
  - [dataLeafPageGetUncompressed](dataLeafPageGetUncompressed.md)
  - memcpy
- Called from (representative examples):
  - [dataBeginPlaceToPageLeaf](dataBeginPlaceToPageLeaf.md)
  - [ginVacuumPostingTreeLeaf](../g/ginVacuumPostingTreeLeaf.md)

## Notes and Other Information
- This is a static function, only accessible within gindatapage.c
- The function allocates memory for the disassembledLeaf structure and its components using palloc/palloc0
- Handles backward compatibility by supporting both compressed and uncompressed page formats
- Sets the oldformat flag to indicate whether the original page was in pre-9.4 format
- The disassembledLeaf structure uses a doubly-linked list (dlist) for efficient segment management
- Located in src/backend/access/gin/gindatapage.c at lines 1370-1443
- Part of the infrastructure for advanced leaf page manipulation operations like splits and vacuuming

## Simplified Source

```c
static disassembledLeaf *
disassembleLeaf(Page page)
{
    disassembledLeaf *leaf = palloc0(sizeof(disassembledLeaf));
    dlist_init(&leaf->segments);

    if (GinPageIsCompressed(page)) {
        // Process compressed page format (9.4+)
        GinPostingList *seg = GinDataLeafPageGetPostingList(page);
        Pointer segend = (Pointer)seg + GinDataLeafPageGetPostingListSize(page);

        // Create segment info for each posting list segment
        while ((Pointer)seg < segend) {
            leafSegmentInfo *seginfo = palloc(sizeof(leafSegmentInfo));
            seginfo->action = GIN_SEGMENT_UNMODIFIED;
            seginfo->seg = seg;
            seginfo->items = NULL;
            seginfo->nitems = 0;
            dlist_push_tail(&leaf->segments, &seginfo->node);

            seg = GinNextPostingListSegment(seg);
        }
        leaf->oldformat = false;
    } else {
        // Process uncompressed page format (pre-9.4)
        ItemPointer uncompressed;
        int nuncompressed;

        uncompressed = dataLeafPageGetUncompressed(page, &nuncompressed);

        if (nuncompressed > 0) {
            leafSegmentInfo *seginfo = palloc(sizeof(leafSegmentInfo));
            seginfo->action = GIN_SEGMENT_REPLACE;
            seginfo->seg = NULL;
            seginfo->items = palloc(nuncompressed * sizeof(ItemPointerData));
            memcpy(seginfo->items, uncompressed, nuncompressed * sizeof(ItemPointerData));
            seginfo->nitems = nuncompressed;
            dlist_push_tail(&leaf->segments, &seginfo->node);
        }
        leaf->oldformat = true;
    }

    return leaf;
}
```