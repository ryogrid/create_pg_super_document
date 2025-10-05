# PageIndexTupleOverwrite

## Location
[src/backend/storage/page/bufpage.c:1405-1509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L1405-L1509)

## Overview
Replaces a specified tuple on an index page in-place, efficiently managing space by shifting other tuples' data while preserving line pointer positions and flags.

## Definition

```c
bool
PageIndexTupleOverwrite(Page page, OffsetNumber offnum,
						Item newtup, Size newsize)
```
## Detailed Description
PageIndexTupleOverwrite provides an efficient mechanism for replacing an existing tuple with a new one at the exact same location on an index page. This function is superior to delete-and-reinsert operations because:

1. **Space optimization**: When tuple sizes are identical, no data movement occurs
2. **Line pointer preservation**: Avoids moving line pointers, maintaining their positions and flags (like LP_DEAD)
3. **Compaction maintenance**: Keeps the page compacted by shifting tuple data as needed
4. **Physical order preservation**: Maintains both logical (ItemId) and physical tuple ordering

The function calculates space requirements, validates the operation feasibility, relocates existing data if necessary, updates affected line pointers, and copies the new tuple data. It returns false if insufficient space is available, making it safe for callers to handle space constraints gracefully.

## Parameters / Member Variables
- `page`: The index page containing the tuple to replace
- `offnum`: The offset number (line pointer index) of the tuple to overwrite
- `newtup`: Pointer to the new tuple data to write
- `newsize`: Size of the new tuple data in bytes
## Dependencies
- Functions called/Symbols referenced:
  - [PageGetMaxOffsetNumber](PageGetMaxOffsetNumber.md)
  - [PageGetItemId](PageGetItemId.md)
  - ItemIdHasStorage
  - ItemIdGetLength
  - ItemIdGetOffset
  - [PageGetItem](PageGetItem.md)
- Called from (representative examples):
  - [brin_doupdate](../b/brin_doupdate.md) (BRIN index tuple updates)
  - [gistplacetopage](../g/gistplacetopage.md) (GiST page tuple placement)
  - [_bt_delitems_vacuum](../b/_bt_delitems_vacuum.md) (B-tree vacuum operations)
  - [_bt_buildadd](../b/_bt_buildadd.md) (B-tree build process)

## Notes and Other Information
- Returns false for insufficient space, elog for corruption errors
- Preserves line pointer flags (especially useful for LP_DEAD bit handling)
- Handles both tuple size increases and decreases efficiently
- Supports items without storage (used by BRIN indexes)
- Calculates size differences and adjusts data layout accordingly
- Essential for in-place tuple updates across multiple index types (BRIN, GiST, B-tree)
- More efficient than separate delete/insert operations when updating existing tuples
- Maintains page compaction automatically during the replacement process

## Simplified Source

```c
bool PageIndexTupleOverwrite(Page page, OffsetNumber offnum,
                           Item newtup, Size newsize) {
    PageHeader phdr = (PageHeader) page;

    // Validate page structure
    if (phdr->pd_lower < SizeOfPageHeaderData ||
        phdr->pd_lower > phdr->pd_upper ||
        phdr->pd_upper > phdr->pd_special) {
        ereport(ERROR, "corrupted page pointers");
    }

    int itemcount = PageGetMaxOffsetNumber(page);
    if (offnum <= 0 || offnum > itemcount) {
        elog(ERROR, "invalid index offnum: %u", offnum);
    }

    // Get existing tuple information
    ItemId tupid = PageGetItemId(page, offnum);
    int oldsize = MAXALIGN(ItemIdGetLength(tupid));
    unsigned offset = ItemIdGetOffset(tupid);
    Size alignednewsize = MAXALIGN(newsize);

    // Check if new tuple fits on page
    if (alignednewsize > oldsize + (phdr->pd_upper - phdr->pd_lower)) {
        return false; // Not enough space
    }

    // Calculate space difference (positive = shrinking, negative = growing)
    int size_diff = oldsize - alignednewsize;

    // Only move data if size changed
    if (size_diff != 0) {
        // Move all tuple data before the target tuple
        char *tuple_space_start = (char *) page + phdr->pd_upper;
        memmove(tuple_space_start + size_diff,
                tuple_space_start,
                offset - phdr->pd_upper);

        // Update page boundary
        phdr->pd_upper += size_diff;

        // Adjust affected line pointer offsets
        for (int i = FirstOffsetNumber; i <= itemcount; i++) {
            ItemId item = PageGetItemId(page, i);
            if (ItemIdHasStorage(item) && ItemIdGetOffset(item) <= offset) {
                item->lp_off += size_diff;
            }
        }
    }

    // Update the target tuple's line pointer and copy new data
    tupid->lp_off = offset + size_diff;
    tupid->lp_len = newsize;
    memcpy(PageGetItem(page, tupid), newtup, newsize);

    return true;
}
```