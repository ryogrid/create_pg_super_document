# gistfillbuffer

## Location
[src/backend/access/gist/gistutil.c:33-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L33-L57)

## Overview
Writes an array of index tuples to a GiST page with no control over free space management, used for efficient bulk insertion of tuples.

## Definition
```c
void gistfillbuffer(Page page, IndexTuple *itup, int len, OffsetNumber off)
```

## Detailed Description
This function performs a straightforward bulk insertion of index tuples into a GiST page. It iterates through the provided array of index tuples and adds each one to the page using PageAddItem. The function does not perform any free space checking or management - it assumes there is sufficient space on the page for all tuples. If any tuple cannot be added (PageAddItem returns InvalidOffsetNumber), it throws an ERROR. The function automatically handles offset number management, either starting from a specified offset or determining the next available offset based on the page's current state.

## Parameters / Member Variables
- `page`: The target page where index tuples will be written
- `itup`: Array of IndexTuple pointers to be inserted into the page
- `len`: Number of tuples in the itup array
- `off`: Starting offset number for insertion; if InvalidOffsetNumber, automatically determines the next available offset

## Dependencies
- Functions called/Symbols referenced:
  - [PageIsEmpty](../P/PageIsEmpty.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - OffsetNumberNext
  - IndexTupleSize
  - PageAddItem
  - FirstOffsetNumber
  - InvalidOffsetNumber
- Called from (representative examples):
  - [gistplacetopage](gistplacetopage.md)
  - [gist_indexsortbuild_levelstate_add](gist_indexsortbuild_levelstate_add.md)
  - [gistRedoPageSplitRecord](gistRedoPageSplitRecord.md)

## Notes and Other Information
This is a low-level utility function that assumes the caller has already verified that sufficient space exists on the page. It's primarily used during page splits, bulk loading operations, and WAL replay where space calculations have been done beforehand. The function will terminate the entire operation with an ERROR if any single tuple cannot be inserted, making it unsuitable for scenarios where graceful handling of space exhaustion is required.

## Simplified Source

```c
void
gistfillbuffer(Page page, IndexTuple *itup, int len, OffsetNumber off)
{
    int i;

    // Determine starting offset if not specified
    if (off == InvalidOffsetNumber)
        off = (PageIsEmpty(page)) ? FirstOffsetNumber :
              OffsetNumberNext(PageGetMaxOffsetNumber(page));

    // Insert each tuple into the page
    for (i = 0; i < len; i++) {
        Size sz = IndexTupleSize(itup[i]);
        OffsetNumber l;

        // Add item to page at current offset
        l = PageAddItem(page, (Item) itup[i], sz, off, false, false);

        // Check for insertion failure
        if (l == InvalidOffsetNumber)
            elog(ERROR, "failed to add item to GiST index page, item %d out of %d, size %d bytes",
                 i, len, (int) sz);

        off++; // Move to next offset
    }
}
```