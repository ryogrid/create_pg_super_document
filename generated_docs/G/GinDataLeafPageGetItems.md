# GinDataLeafPageGetItems

## Location
[src/backend/access/gin/gindatapage.c:135-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gindatapage.c#L135-L181)

## Overview
Extracts and returns all TIDs (tuple identifiers) from a GIN data leaf page in ascending order, supporting both compressed and uncompressed page formats.

## Definition

```c
ItemPointer
GinDataLeafPageGetItems(Page page, int *nitems, ItemPointerData advancePast)
```
## Detailed Description
This function reads TIDs from a GIN (Generalized Inverted Index) data leaf page and returns them as a single uncompressed array in ascending order. The function handles both compressed and uncompressed page formats automatically.

For compressed pages, it processes posting list segments, optionally skipping segments that contain only TIDs less than or equal to the `advancePast` hint. This optimization allows callers to efficiently retrieve only TIDs of interest when scanning forward through the index.

For uncompressed pages, it simply copies the existing uncompressed TID array.

The function provides an important optimization through the `advancePast` parameter, which serves as a hint to skip posting lists that contain only TIDs the caller has already processed.

## Parameters / Member Variables
- `page`: The GIN data leaf page to extract TIDs from
- `nitems`: Output parameter that receives the number of TIDs extracted
- `advancePast`: Hint indicating caller is only interested in TIDs > advancePast; use ItemPointerSetMin to return all items

## Dependencies
- Functions called/Symbols referenced:
  - GinPageIsCompressed
  - GinDataLeafPageGetPostingList
  - GinDataLeafPageGetPostingListSize
  - GinNextPostingListSegment
  - [ginCompareItemPointers](../g/ginCompareItemPointers.md)
  - [ginPostingListDecodeAllSegments](../g/ginPostingListDecodeAllSegments.md)
  - [dataLeafPageGetUncompressed](../d/dataLeafPageGetUncompressed.md)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
- Called from (representative examples):
  - [startScanEntry](../s/startScanEntry.md)
  - [entryLoadMoreItems](../e/entryLoadMoreItems.md)
  - [GinBtreeDataLeafInsertData](GinBtreeDataLeafInsertData.md)

## Notes and Other Information
- The function may still return items smaller than `advancePast` that are in the same posting list as items of interest, so callers must validate all returned items
- For compressed pages, the function performs segment-level skipping for efficiency
- The returned ItemPointer array is allocated with palloc() and must be freed by the caller
- TIDs are guaranteed to be returned in ascending order regardless of page format

## Simplified Source

```c
ItemPointer GinDataLeafPageGetItems(Page page, int *nitems, ItemPointerData advancePast) {
    ItemPointer result;

    if (GinPageIsCompressed(page)) {
        // Handle compressed page format
        GinPostingList *seg = GinDataLeafPageGetPostingList(page);
        Size len = GinDataLeafPageGetPostingListSize(page);
        Pointer endptr = ((Pointer) seg) + len;

        // Skip segments with only TIDs <= advancePast (optimization)
        if (ItemPointerIsValid(&advancePast)) {
            GinPostingList *next = GinNextPostingListSegment(seg);
            while ((Pointer) next < endptr &&
                   ginCompareItemPointers(&next->first, &advancePast) <= 0) {
                seg = next;
                next = GinNextPostingListSegment(seg);
            }
            len = endptr - (Pointer) seg;
        }

        // Decode remaining segments into TID array
        if (len > 0) {
            result = ginPostingListDecodeAllSegments(seg, len, nitems);
        } else {
            result = NULL;
            *nitems = 0;
        }
    } else {
        // Handle uncompressed page format - just copy the TID array
        ItemPointer uncompressed_tids = dataLeafPageGetUncompressed(page, nitems);
        result = palloc((*nitems) * sizeof(ItemPointerData));
        memcpy(result, uncompressed_tids, (*nitems) * sizeof(ItemPointerData));
    }

    return result;
}
```