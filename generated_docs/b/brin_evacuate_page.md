# brin_evacuate_page

## Location
[src/backend/access/brin/brin_pageops.c:564-623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_pageops.c#L564-L623)

## Overview
Moves all tuples out of a BRIN index page that has been marked for evacuation, relocating them to appropriate locations within the index.

## Definition
```c
void brin_evacuate_page(Relation idxRel, BlockNumber pagesPerRange,
                        BrinRevmap *revmap, Buffer buf)
```

## Detailed Description
This function performs the actual evacuation of tuples from a BRIN index page that was previously marked for evacuation by brin_start_evacuating_page. It systematically processes all valid tuples on the page and relocates them using the BRIN update mechanism.

The evacuation process involves:
1. Asserting that the page has the BRIN_EVACUATE_PAGE flag set
2. Iterating through all item pointers on the page
3. For each used tuple, creating a copy and attempting to relocate it via brin_doupdate
4. Handling lock management during the update process (unlock for update, relock for continued iteration)
5. Checking for interrupts to allow cancellation of long-running operations
6. Handling cases where the page might be converted to a revmap page during evacuation

The function uses a retry mechanism when brin_doupdate fails, decrementing the offset counter to re-process the same slot. It also handles the possibility that the page might be converted to a revmap page by another process during evacuation.

## Parameters / Member Variables
- `idxRel`: Relation structure representing the BRIN index
- `pagesPerRange`: Number of heap pages covered by each BRIN index tuple, used for tuple placement calculations
- `revmap`: BrinRevmap structure for managing reverse mapping operations
- `buf`: Buffer containing the page to evacuate, must be locked by caller

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md) (to access the page from buffer)
  - BrinPageFlags (to access and verify page flags)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md) (to get the highest item offset)
  - [PageGetItemId](../P/PageGetItemId.md) (to access item pointers)
  - ItemIdIsUsed (to check if an item pointer is in use)
  - ItemIdGetLength (to get tuple size)
  - [PageGetItem](../P/PageGetItem.md) (to access tuple data)
  - [brin_copy_tuple](brin_copy_tuple.md) (to create a copy of the tuple)
  - [LockBuffer](../L/LockBuffer.md) (for lock management during updates)
  - [brin_doupdate](brin_doupdate.md) (to relocate the tuple)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md) (to release the buffer)
  - CHECK_FOR_INTERRUPTS (to allow query cancellation)
  - BRIN_IS_REGULAR_PAGE (to check if page is still a regular index page)
  - BRIN_EVACUATE_PAGE (flag constant)
  - BUFFER_LOCK_UNLOCK, BUFFER_LOCK_SHARE (lock type constants)
- Called from:
  - [revmap_physical_extend](../r/revmap_physical_extend.md) (in brin_revmap.c)

## Notes and Other Information
- The caller must hold a lock on the buffer before calling this function
- The function releases both the lock and pin on the buffer before returning
- Uses a retry mechanism when tuple relocation fails, indicated by brin_doupdate returning false
- Handles concurrent operations that might convert the page to a revmap page during evacuation
- The function checks for interrupts to allow cancellation during long-running evacuations
- Part of the BRIN page repurposing mechanism that allows regular index pages to be converted for reverse mapping use
- The evacuation process maintains index consistency by ensuring all tuples are properly relocated before the page is repurposed

## Simplified Source

```c
void brin_evacuate_page(Relation idxRel, BlockNumber pagesPerRange,
                        BrinRevmap *revmap, Buffer buf)
{
    Page page = BufferGetPage(buf);
    BrinTuple *btup = NULL;
    Size btupsz = 0;

    // Verify page is marked for evacuation
    Assert(BrinPageFlags(page) & BRIN_EVACUATE_PAGE);

    // Process each tuple on the page
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
    {
        CHECK_FOR_INTERRUPTS();

        ItemId lp = PageGetItemId(page, off);
        if (ItemIdIsUsed(lp))
        {
            // Copy tuple data
            Size sz = ItemIdGetLength(lp);
            BrinTuple *tup = (BrinTuple *) PageGetItem(page, lp);
            tup = brin_copy_tuple(tup, sz, btup, &btupsz);

            // Relocate tuple to new location
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            if (!brin_doupdate(idxRel, pagesPerRange, revmap, tup->bt_blkno,
                              buf, off, tup, sz, tup, sz, false))
                off--;  // Retry on failure

            LockBuffer(buf, BUFFER_LOCK_SHARE);

            // Check if page was converted to revmap during evacuation
            if (!BRIN_IS_REGULAR_PAGE(page))
                break;
        }
    }

    UnlockReleaseBuffer(buf);
}
```