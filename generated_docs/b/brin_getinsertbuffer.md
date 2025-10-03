# brin_getinsertbuffer

## Location
[src/backend/access/brin/brin_pageops.c:690-883](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_pageops.c#L690-L883)

## Overview
Returns a pinned and exclusively locked buffer suitable for inserting a BRIN index item, handling page allocation, extension, and locking coordination with existing buffers.

## Definition
```c
static Buffer brin_getinsertbuffer(Relation irel, Buffer oldbuf, Size itemsz,
                                   bool *extended)
```

## Detailed Description
This function is the core buffer management routine for BRIN index insertions, responsible for finding or creating a suitable page with enough free space for a new index item. It implements sophisticated logic to handle multiple scenarios including page reuse, relation extension, and coordination between old and new buffers.

The function operates through several key phases:

1. **Target Page Selection**: Uses the relation's target block hint or consults the Free Space Map (FSM) to find a candidate page
2. **Buffer Acquisition Loop**: Iteratively attempts to find a suitable buffer until successful
3. **Locking Coordination**: Implements deadlock-avoidance by locking buffers in block number order
4. **Revmap Detection**: Checks if pages have been converted to revmap pages during concurrent operations
5. **Extension Handling**: Extends the relation when no existing page has sufficient space
6. **Space Validation**: Verifies that the selected page actually has enough free space

The function handles several complex scenarios:
- **Concurrent revmap extension**: Detects when pages are converted to revmap use and returns InvalidBuffer
- **FSM inconsistencies**: Updates FSM when pages don't have expected free space
- **Relation extension**: Creates new pages when needed, with proper initialization and FSM updates
- **Buffer coordination**: Manages locking order between old and new buffers to prevent deadlocks

## Parameters / Member Variables
- `irel`: Relation structure representing the BRIN index
- `oldbuf`: Existing buffer that may also need locking (can be InvalidBuffer)
- `itemsz`: Size of the item to be inserted, must not exceed BrinMaxItemSize
- `extended`: Output parameter set to true if the relation was extended to create a new page

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (to get block numbers from buffers)
  - RelationGetTargetBlock/RelationSetTargetBlock (for target block management)
  - [GetPageWithFreeSpace](../G/GetPageWithFreeSpace.md) (to query FSM for pages with free space)
  - [ReadBuffer](../R/ReadBuffer.md) (to read pages into buffers)
  - [LockBuffer](../L/LockBuffer.md)/UnlockReleaseBuffer (for buffer locking)
  - [LockRelationForExtension](../L/LockRelationForExtension.md)/UnlockRelationForExtension (for extension coordination)
  - [BufferGetPage](../B/BufferGetPage.md) (to access page data)
  - [br_page_get_freespace](br_page_get_freespace.md) (to measure available space)
  - [brin_initialize_empty_new_buffer](brin_initialize_empty_new_buffer.md) (to initialize new pages)
  - [RecordAndGetPageWithFreeSpace](../R/RecordAndGetPageWithFreeSpace.md) (to update FSM and find new pages)
  - [FreeSpaceMapVacuumRange](../F/FreeSpaceMapVacuumRange.md) (to update FSM for extended pages)
  - BRIN_IS_REGULAR_PAGE (to check page type)
  - RELATION_IS_LOCAL (to check relation locality)
  - BrinMaxItemSize (maximum item size constant)
  - P_NEW (special block number for extension)
  - Various lock and buffer constants
- Called from:
  - [brin_doupdate](brin_doupdate.md) (for tuple updates)
  - [brin_doinsert](brin_doinsert.md) (for tuple insertions)

## Notes and Other Information
- This is a static function internal to brin_pageops.c
- Implements deadlock avoidance by always locking buffers in ascending block number order
- The caller is responsible for initializing extended pages and updating FSM after insertion
- Handles the corner case where FSM suggests a page that has been converted to revmap use
- May extend the relation but not return the new page if concurrent revmap extension occurs
- The function ensures that extended pages are properly initialized and recorded in FSM even when not returned
- Contains detailed error handling for oversized items that exceed the maximum page capacity
- Uses CHECK_FOR_INTERRUPTS to allow cancellation during potentially long-running operations
- The extension lock is held only during the critical section of relation extension to minimize contention
- Returns InvalidBuffer when the old buffer is found to be converted to a revmap page, signaling the caller to restart the operation

## Simplified Source

```c
static Buffer brin_getinsertbuffer(Relation irel, Buffer oldbuf, Size itemsz,
                                   bool *extended)
{
    BlockNumber oldblk, newblk;
    Page page;
    Size freespace;

    Assert(itemsz <= BrinMaxItemSize);

    // Get old buffer's block number
    oldblk = BufferIsValid(oldbuf) ? BufferGetBlockNumber(oldbuf) : InvalidBlockNumber;

    // Find target page using FSM or relation target block
    newblk = RelationGetTargetBlock(irel);
    if (newblk == InvalidBlockNumber)
        newblk = GetPageWithFreeSpace(irel, itemsz);

    // Loop until we find a suitable page
    for (;;)
    {
        Buffer buf;
        bool extensionLockHeld = false;

        CHECK_FOR_INTERRUPTS();
        *extended = false;

        // Handle page acquisition
        if (newblk == InvalidBlockNumber)
        {
            // Extend relation for new page
            if (!RELATION_IS_LOCAL(irel))
            {
                LockRelationForExtension(irel, ExclusiveLock);
                extensionLockHeld = true;
            }
            buf = ReadBuffer(irel, P_NEW);
            newblk = BufferGetBlockNumber(buf);
            *extended = true;
        }
        else if (newblk == oldblk)
        {
            buf = oldbuf;  // Reuse old buffer
        }
        else
        {
            buf = ReadBuffer(irel, newblk);
        }

        // Lock buffers in order to avoid deadlocks
        if (BufferIsValid(oldbuf) && oldblk < newblk)
        {
            LockBuffer(oldbuf, BUFFER_LOCK_EXCLUSIVE);
            if (!BRIN_IS_REGULAR_PAGE(BufferGetPage(oldbuf)))
            {
                // Old page converted to revmap - clean up and return invalid
                LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);
                if (*extended)
                    brin_initialize_empty_new_buffer(irel, buf);
                if (extensionLockHeld)
                    UnlockRelationForExtension(irel, ExclusiveLock);
                ReleaseBuffer(buf);
                return InvalidBuffer;
            }
        }

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        if (extensionLockHeld)
            UnlockRelationForExtension(irel, ExclusiveLock);

        page = BufferGetPage(buf);

        // Check if page has enough space
        freespace = *extended ? BrinMaxItemSize : br_page_get_freespace(page);
        if (freespace >= itemsz)
        {
            RelationSetTargetBlock(irel, newblk);

            // Lock old buffer if needed
            if (BufferIsValid(oldbuf) && oldblk > newblk)
                LockBuffer(oldbuf, BUFFER_LOCK_EXCLUSIVE);

            return buf;
        }

        // Page doesn't have enough space - try again
        if (*extended)
        {
            brin_initialize_empty_new_buffer(irel, buf);
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                           errmsg("index row size %zu exceeds maximum %zu for index \"%s\"",
                                 itemsz, freespace, RelationGetRelationName(irel))));
        }

        // Clean up and find new page
        if (newblk != oldblk)
            UnlockReleaseBuffer(buf);
        if (BufferIsValid(oldbuf) && oldblk <= newblk)
            LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);

        newblk = RecordAndGetPageWithFreeSpace(irel, newblk, freespace, itemsz);
    }
}
```