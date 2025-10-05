# PageIndexTupleDeleteNoCompact

## Location
[src/backend/storage/page/bufpage.c:1295-1404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L1295-L1404)

## Overview
Removes a specified tuple from an index page by marking its line pointer as unused rather than compacting it out, preserving existing tuple identifier (TID) stability.

## Definition

```c
void
PageIndexTupleDeleteNoCompact(Page page, OffsetNumber offnum)
```
## Detailed Description
PageIndexTupleDeleteNoCompact provides a specialized deletion mechanism for index access methods that require existing TIDs of live tuples to remain unchanged. Instead of compacting out the deleted tuple and shifting line pointers (which would change TIDs), this function:

1. Marks the line pointer as "unused" if it's not the last one on the page
2. Removes the line pointer entirely only if it's the last one (safe for TID preservation)
3. Moves tuple data to reclaim space while adjusting remaining line pointer offsets
4. Maintains page structure integrity without TID renumbering

This approach trades some space efficiency for TID stability, which is crucial for certain index types that rely on persistent tuple references.

## Parameters / Member Variables
- `page`: The index page containing the tuple to delete
- `offnum`: The offset number (line pointer index) of the tuple to delete
## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](PageGetMaxOffsetNumber.md)
  - [PageGetItemId](PageGetItemId.md)
  - ItemIdHasStorage
  - ItemIdGetLength
  - ItemIdGetOffset
  - ItemIdSetUnused
  - [PageIsEmpty](PageIsEmpty.md)
- Called from (representative examples):
  - [brin_doupdate](../b/brin_doupdate.md) (BRIN index tuple updates)
  - [brinRevmapDesummarizeRange](../b/brinRevmapDesummarizeRange.md) (BRIN revmap operations)
  - [brin_xlog_update](../b/brin_xlog_update.md) (BRIN WAL replay)
  - [brin_xlog_desummarize_page](../b/brin_xlog_desummarize_page.md) (BRIN WAL desummarization)

## Notes and Other Information
- Primary use case: Index access methods requiring TID stability (notably BRIN indexes)
- Unlike PageIndexTupleDelete, does not compact line pointer array to preserve TID values
- Only removes line pointer if it's the last one on the page (safe operation)
- Reclaims tuple space by moving data and adjusting remaining line pointer offsets
- Includes comprehensive page corruption validation
- More space-wasteful than compact deletion but necessary for TID-sensitive operations
- Line pointers marked as unused can potentially be reused for future insertions

## Simplified Source

```c
void PageIndexTupleDeleteNoCompact(Page page, OffsetNumber offnum) {
    PageHeader phdr = (PageHeader) page;

    // Validate page structure
    if (phdr->pd_lower < SizeOfPageHeaderData ||
        phdr->pd_lower > phdr->pd_upper ||
        phdr->pd_upper > phdr->pd_special) {
        ereport(ERROR, "corrupted page pointers");
    }

    int nline = PageGetMaxOffsetNumber(page);
    if (offnum <= 0 || offnum > nline) {
        elog(ERROR, "invalid index offnum: %u", offnum);
    }

    // Get tuple information
    ItemId tup = PageGetItemId(page, offnum);
    Size size = MAXALIGN(ItemIdGetLength(tup));
    unsigned offset = ItemIdGetOffset(tup);

    // Handle line pointer: mark unused or remove if last
    if (offnum < nline) {
        ItemIdSetUnused(tup); // Mark as unused to preserve TIDs
    } else {
        phdr->pd_lower -= sizeof(ItemIdData); // Safe to remove last pointer
        nline--;
    }

    // Move tuple data to reclaim space
    char *tuple_space_start = (char *) page + phdr->pd_upper;
    if (offset > phdr->pd_upper) {
        memmove(tuple_space_start + size,
                tuple_space_start,
                offset - phdr->pd_upper);
    }

    // Update page boundary
    phdr->pd_upper += size;

    // Adjust remaining line pointer offsets
    if (!PageIsEmpty(page)) {
        for (int i = 1; i <= nline; i++) {
            ItemId item = PageGetItemId(page, i);
            if (ItemIdHasStorage(item) && ItemIdGetOffset(item) <= offset) {
                item->lp_off += size;
            }
        }
    }
}
```