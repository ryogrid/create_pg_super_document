# terminate_brin_buildstate

## Location
[src/backend/access/brin/brin.c:1707-1751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L1707-L1751)

## Overview
Releases all resources associated with a BrinBuildState structure, including buffers, descriptors, and memory allocations used during BRIN index construction.

## Definition
```c
static void terminate_brin_buildstate(BrinBuildState *state)
```

## Detailed Description
This static function performs cleanup operations for a BrinBuildState that was used during BRIN index building or maintenance. It properly releases the current insert buffer if one exists, records any remaining free space in the Free Space Map for efficient future use, frees the BRIN tuple descriptor and associated memory tuple, and finally deallocates the state structure itself. This ensures proper resource management and prevents memory leaks during BRIN operations.

## Parameters / Member Variables
- `state`: Pointer to the BrinBuildState structure to be cleaned up and deallocated

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsInvalid
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageGetFreeSpace](../P/PageGetFreeSpace.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
  - [RecordPageWithFreeSpace](../R/RecordPageWithFreeSpace.md)
  - [FreeSpaceMapVacuumRange](../F/FreeSpaceMapVacuumRange.md)
  - [brin_free_desc](../b/brin_free_desc.md)
  - [pfree](../p/pfree.md)
- Types referenced:
  - [BrinBuildState](../B/BrinBuildState.md)
  - Page
  - Size
  - BlockNumber
- Called from (representative examples):
  - [brinbuild](../b/brinbuild.md)
  - [brinsummarize](../b/brinsummarize.md)

## Notes and Other Information
- This is a static function only accessible within the brin.c module
- The function ensures that any remaining free space in the last used index buffer is recorded in the Free Space Map
- Proper cleanup includes both buffer management and memory deallocation
- The Free Space Map vacuum operation helps maintain accurate free space information for future insertions
- Essential for preventing resource leaks during BRIN index operations

## Simplified Source

```c
static void
terminate_brin_buildstate(BrinBuildState *state)
{
    // Handle the last insert buffer if it exists
    if (!BufferIsInvalid(state->bs_currentInsertBuf)) {
        Page page;
        Size freespace;
        BlockNumber blk;

        // Record remaining free space in FSM
        page = BufferGetPage(state->bs_currentInsertBuf);
        freespace = PageGetFreeSpace(page);
        blk = BufferGetBlockNumber(state->bs_currentInsertBuf);
        ReleaseBuffer(state->bs_currentInsertBuf);
        RecordPageWithFreeSpace(state->bs_irel, blk, freespace);
        FreeSpaceMapVacuumRange(state->bs_irel, blk, blk + 1);
    }

    // Free descriptor and tuple structures
    brin_free_desc(state->bs_bdesc);
    pfree(state->bs_dtuple);
    pfree(state);
}
```