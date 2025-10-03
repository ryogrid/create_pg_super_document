# gistextractpage

## Location
[src/backend/access/gist/gistutil.c:94-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L94-L112)

## Overview
Extracts all index tuples from a GiST page and returns them as an array of IndexTuple pointers.

## Definition
```c
IndexTuple *gistextractpage(Page page, int *len /* out */)
```

## Detailed Description
This function performs a complete extraction of all index tuples from a GiST page, creating an array of IndexTuple pointers that reference the tuples in their original page locations. It determines the maximum offset number on the page, allocates an array to hold pointers to all tuples, and then iterates through each valid offset to extract the corresponding tuple. The function does not create copies of the tuples - it simply builds an array of pointers to the existing tuples within the page buffer. This makes it efficient for operations that need to process all tuples on a page without modifying them.

## Parameters / Member Variables
- `page`: The source page from which to extract tuples
- `len`: Output parameter that receives the number of tuples extracted (set to maxoff)

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [palloc](../p/palloc.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - FirstOffsetNumber
  - OffsetNumberNext
- Called from (representative examples):
  - [gistplacetopage](gistplacetopage.md)
  - [gist_indexsortbuild_levelstate_flush](gist_indexsortbuild_levelstate_flush.md)

## Notes and Other Information
The returned array contains pointers to tuples that remain within the original page buffer, so the tuples are only valid as long as the page buffer remains pinned and unmodified. This function is commonly used during page reorganization operations, page splits, and bulk loading where all tuples on a page need to be processed collectively. The caller is responsible for freeing the allocated array (but not the individual tuples, as they remain in the page). The array indexing starts from 0, with itvec[0] corresponding to the tuple at FirstOffsetNumber.

## Simplified Source

```c
IndexTuple *gistextractpage(Page page, int *len) {
    // Get number of tuples on page
    OffsetNumber max_offset = PageGetMaxOffsetNumber(page);
    *len = max_offset;

    // Allocate array for tuple pointers
    IndexTuple *tuple_array = palloc(sizeof(IndexTuple) * max_offset);

    // Extract each tuple and store pointer in array
    for (OffsetNumber i = FirstOffsetNumber; i <= max_offset; i++) {
        tuple_array[i - FirstOffsetNumber] = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
    }

    return tuple_array;
}
```