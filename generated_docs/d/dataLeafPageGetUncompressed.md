# dataLeafPageGetUncompressed

## Location
[src/backend/access/gin/gindatapage.c:211-233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L211-L233)

## Overview
Returns a pointer to the uncompressed TID array on pre-9.4 format uncompressed GIN data leaf pages.

## Definition
```c
static ItemPointer dataLeafPageGetUncompressed(Page page, int *nitems)
```

## Detailed Description
This static function provides access to the raw uncompressed TID array stored on legacy pre-PostgreSQL 9.4 format GIN data leaf pages. In the old page format, the entire page content after the header was used to store an uncompressed array of ItemPointer structures (TIDs).

The function performs an assertion to ensure the page is indeed uncompressed, then retrieves the pointer to the data area and extracts the item count from the page's opaque area. This function is part of PostgreSQL's backward compatibility layer for reading older GIN index formats.

## Parameters / Member Variables
- `page`: The uncompressed GIN data leaf page to access
- `nitems`: Output parameter that receives the number of TIDs in the array

## Dependencies
- Functions called/Symbols referenced:
  - GinPageIsCompressed
  - GinDataPageGetData
  - GinPageGetOpaque
- Called from (representative examples):
  - leafSegmentInfo
  - [GinDataLeafPageGetItems](../G/GinDataLeafPageGetItems.md)
  - [GinDataLeafPageGetItemsToTbm](../G/GinDataLeafPageGetItemsToTbm.md)
  - [disassembleLeaf](disassembleLeaf.md)

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit
- Only works with pre-9.4 format uncompressed pages; compressed pages use a different layout
- The function includes an assertion to verify the page is not compressed
- In the old format, the maxoff field in the page opaque area stores the number of items
- This function provides backward compatibility for reading legacy GIN index formats
- The returned pointer points directly into the page buffer, so the data should not be modified

## Simplified Source

```c
static ItemPointer dataLeafPageGetUncompressed(Page page, int *nitems) {
    // Verify this is an uncompressed page (old pre-9.4 format)
    Assert(!GinPageIsCompressed(page));

    // Get pointer to the TID array (starts right after page header)
    ItemPointer items = (ItemPointer) GinDataPageGetData(page);

    // In old format, item count is stored in the page opaque area
    *nitems = GinPageGetOpaque(page)->maxoff;

    return items;
}
```