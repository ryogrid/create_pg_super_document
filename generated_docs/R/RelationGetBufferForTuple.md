# RelationGetBufferForTuple

## Location
[src/backend/access/heap/hio.c:502-885](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/hio.c#L502-L885)

## Overview
RelationGetBufferForTuple finds and returns a pinned, exclusive-locked buffer containing a page with sufficient free space for tuple insertion, handling complex buffer coordination, visibility map management, and relation extension.

## Definition

```c
Buffer
RelationGetBufferForTuple(Relation relation, Size len,
						  Buffer otherBuffer, int options,
						  BulkInsertState bistate,
						  Buffer *vmbuffer, Buffer *vmbuffer_other,
						  int num_pages)
```
## Detailed Description
This is a comprehensive function responsible for obtaining a suitable buffer for heap tuple insertion. It implements sophisticated logic for:

**Buffer Selection Strategy:**
- First tries cached target page from BulkInsertState or relation cache
- Falls back to Free Space Map (FSM) for finding pages with adequate space
- Attempts the last page of relation before extending
- Extends relation when no existing page has sufficient space

**Locking and Deadlock Prevention:**
- Handles complex buffer locking scenarios with proper ordering (ascending page numbers)
- Coordinates with otherBuffer to prevent deadlocks in concurrent operations
- Manages visibility map pins that must be acquired before buffer locks

**Space Management:**
- Respects fillfactor settings while allowing large tuples in nearly-empty pages
- Updates FSM with actual page free space information
- Supports bulk extension for efficient multi-page allocation

**Special Features:**
- HEAP_INSERT_SKIP_FSM option for bypassing FSM during bulk loads
- HEAP_INSERT_FROZEN support for frozen tuple insertion
- Bulk insert optimization through BulkInsertState caching
- Proper handling of all-visible page flag clearing

## Parameters / Member Variables
- : Target relation for tuple insertion
- : Required free space for the new tuple (will be MAXALIGN'd)
- : Previously pinned buffer for deadlock prevention (InvalidBuffer if none)
- : Insertion options (HEAP_INSERT_SKIP_FSM, HEAP_INSERT_FROZEN, etc.)
- : Bulk insert state for optimization (NULL for single inserts)
- : Input/output parameter for visibility map buffer of target page
- : Input/output parameter for visibility map buffer of otherBuffer
- : Number of pages to extend relation by if extension is needed (minimum 1)

## Dependencies
- Functions called/Symbols referenced:
  - [ReadBufferBI](ReadBufferBI.md), ReadBuffer, ReadBufferExtended
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md), BufferGetPage, BufferGetPageSize
  - [PageIsAllVisible](../P/PageIsAllVisible.md), PageIsNew, PageInit, PageGetHeapFreeSpace, PageGetMaxOffsetNumber
  - [GetVisibilityMapPins](../G/GetVisibilityMapPins.md), visibilitymap_pin, visibilitymap_pin_ok
  - [LockBuffer](../L/LockBuffer.md), ConditionalLockBuffer, ReleaseBuffer, UnlockReleaseBuffer
  - RelationGetTargetBlock, RelationSetTargetBlock, RelationAddBlocks
  - [GetPageWithFreeSpace](../G/GetPageWithFreeSpace.md), RecordPageWithFreeSpace, RecordAndGetPageWithFreeSpace
  - RelationGetTargetPageFreeSpace, RelationGetNumberOfBlocks
- Called from:
  - [heap_insert](../h/heap_insert.md)
  - [heap_multi_insert](../h/heap_multi_insert.md)  
  - [heap_update](../h/heap_update.md)

## Notes and Other Information
- Central function in PostgreSQL's heap insertion mechanism
- Implements careful lock ordering to prevent deadlocks (buffers locked in ascending page order)
- The function can release and reacquire locks during visibility map pin operations
- Handles race conditions where page state changes during lock/pin operations
- Supports both single tuple insertion and bulk insert optimization
- EREPORT(ERROR) is allowed, unlike lower-level functions like RelationPutHeapTuple
- [Complex](../C/Complex.md) retry logic handles cases where target buffer state changes during processing
- The function maintains relation's target block cache for insertion locality

## Simplified Source

```c
Buffer RelationGetBufferForTuple(Relation relation, Size len,
                                 Buffer otherBuffer, int options,
                                 BulkInsertState bistate,
                                 Buffer *vmbuffer, Buffer *vmbuffer_other,
                                 int num_pages)
{
    bool use_fsm = !(options & HEAP_INSERT_SKIP_FSM);
    Buffer buffer = InvalidBuffer;
    Page page;
    Size targetFreeSpace;
    BlockNumber targetBlock, otherBlock;

    len = MAXALIGN(len);

    // Validate tuple size
    if (len > MaxHeapTupleSize)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("row is too big: size %zu, maximum size %zu",
                               len, MaxHeapTupleSize)));

    // Calculate required free space considering fillfactor
    Size saveFreeSpace = RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR);
    Size nearlyEmptyFreeSpace = MaxHeapTupleSize - (MaxHeapTuplesPerPage / 8 * sizeof(ItemIdData));

    if (len + saveFreeSpace > nearlyEmptyFreeSpace)
        targetFreeSpace = Max(len, nearlyEmptyFreeSpace);
    else
        targetFreeSpace = len + saveFreeSpace;

    // Get other buffer's block number for lock ordering
    if (otherBuffer != InvalidBuffer)
        otherBlock = BufferGetBlockNumber(otherBuffer);

    // Try cached target block first
    if (bistate && bistate->current_buf != InvalidBuffer)
        targetBlock = BufferGetBlockNumber(bistate->current_buf);
    else
        targetBlock = RelationGetTargetBlock(relation);

    // If no cached target and using FSM, get suggestion from FSM
    if (targetBlock == InvalidBlockNumber && use_fsm)
        targetBlock = GetPageWithFreeSpace(relation, targetFreeSpace);

    // If FSM has no suggestion, try the last page
    if (targetBlock == InvalidBlockNumber)
    {
        BlockNumber nblocks = RelationGetNumberOfBlocks(relation);
        if (nblocks > 0)
            targetBlock = nblocks - 1;
    }

loop:
    // Try existing pages
    while (targetBlock != InvalidBlockNumber)
    {
        // Lock buffers in proper order to prevent deadlocks
        if (otherBuffer == InvalidBuffer)
        {
            // Simple case: only one buffer
            buffer = ReadBufferBI(relation, targetBlock, RBM_NORMAL, bistate);
            if (PageIsAllVisible(BufferGetPage(buffer)))
                visibilitymap_pin(relation, targetBlock, vmbuffer);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        }
        else
        {
            // Complex case: coordinate two buffers
            // Lock in ascending block number order
            if (otherBlock < targetBlock)
            {
                buffer = ReadBuffer(relation, targetBlock);
                LockBuffer(otherBuffer, BUFFER_LOCK_EXCLUSIVE);
                LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
            }
            else
            {
                buffer = ReadBuffer(relation, targetBlock);
                LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
                LockBuffer(otherBuffer, BUFFER_LOCK_EXCLUSIVE);
            }
        }

        // Handle visibility map pins
        GetVisibilityMapPins(relation, buffer, otherBuffer,
                             targetBlock, otherBlock, vmbuffer, vmbuffer_other);

        page = BufferGetPage(buffer);

        // Initialize page if new
        if (PageIsNew(page))
        {
            PageInit(page, BufferGetPageSize(buffer), 0);
            MarkBufferDirty(buffer);
        }

        // Check if page has enough space
        Size pageFreeSpace = PageGetHeapFreeSpace(page);
        if (targetFreeSpace <= pageFreeSpace)
        {
            // Found suitable page
            RelationSetTargetBlock(relation, targetBlock);
            return buffer;
        }

        // Not enough space, unlock and try next page
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        if (otherBuffer != InvalidBuffer && otherBlock != targetBlock)
            LockBuffer(otherBuffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);

        // Get next candidate page
        if (use_fsm)
            targetBlock = RecordAndGetPageWithFreeSpace(relation, targetBlock,
                                                        pageFreeSpace, targetFreeSpace);
        else
            break; // Without FSM, extend the relation
    }

    // No existing page has enough space, extend the relation
    buffer = RelationAddBlocks(relation, bistate, num_pages, use_fsm, NULL);
    targetBlock = BufferGetBlockNumber(buffer);

    // Handle locking for extended page
    if (otherBuffer != InvalidBuffer)
        LockBuffer(otherBuffer, BUFFER_LOCK_EXCLUSIVE);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    // Handle visibility map pins for new page
    if (options & HEAP_INSERT_FROZEN)
        visibilitymap_pin(relation, targetBlock, vmbuffer);

    // Verify space is still available (rare race condition)
    page = BufferGetPage(buffer);
    Size pageFreeSpace = PageGetHeapFreeSpace(page);
    if (len > pageFreeSpace)
        goto loop; // Retry if space was consumed

    // Set as target for future insertions
    RelationSetTargetBlock(relation, targetBlock);

    return buffer;
}
```