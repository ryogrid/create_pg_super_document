# LogicalTapeFreeze

## Location
[src/backend/utils/sort/logtape.c:981-1061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L981-L1061)

## Overview
LogicalTapeFreeze transitions a logical tape from write mode to a frozen read mode, allowing the tape contents to be read multiple times and/or read backwards, primarily used for final merge output passes.

## Definition
```c
void LogicalTapeFreeze(LogicalTape *lt, TapeShare *share)
```

## Detailed Description
LogicalTapeFreeze performs the critical transition of a logical tape from write mode to a frozen, reusable read state. This function must be called at the end of a write pass, before rewinding the tape. It flushes any pending write data, switches the tape to read mode, and sets up the tape for multiple reads or backward reading operations.

The freezing process includes:
1. Flushing any dirty buffer contents to storage
2. Marking the tape as non-writing and frozen
3. Resizing the buffer to a single block for optimal seek/backspace operations
4. Reading the first block and setting up read state
5. Optionally preparing metadata for parallel sort coordination

The frozen state ensures tape contents remain available until the LogicalTapeSet is destroyed, making it suitable for final merge passes where data may need to be accessed multiple times.

## Parameters / Member Variables
- `lt`: Pointer to the LogicalTape structure to freeze
- `share`: Optional output parameter (can be NULL) that receives storage metadata for sharing tape contents across processes in parallel sorts

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalTapeSet](LogicalTapeSet.md), TapeShare (structure types)
  - VALGRIND_MAKE_MEM_DEFINED (memory debugging support)
  - TapeBlockSetNBytes, TapeBlockGetNBytes (block size management)
  - TapeBlockIsLast, TapeBlockGetTrailer (block metadata access)
  - [ltsWriteBlock](../l/ltsWriteBlock.md), ltsReadBlock (low-level block I/O)
  - [BufFileExportFileSet](../B/BufFileExportFileSet.md) (file sharing for parallel operations)
- Called from (representative examples):
  - [mergeruns](../m/mergeruns.md) (in tuplesort.c during merge operations)
  - [worker_freeze_result_tape](../w/worker_freeze_result_tape.md) (in tuplesort.c for parallel sort coordination)

## Notes and Other Information
- Must be called exactly at the end of a write pass, before rewinding
- Performs rewind and mode switch automatically - subsequent rewind calls are unnecessary but harmless
- Resizes buffer to single block (BLCKSZ) for optimal seek/backspace performance
- Handles empty tapes gracefully by setting appropriate state
- The share parameter enables coordination in parallel sorts by exporting file metadata
- Once frozen, tape contents are preserved until the entire LogicalTapeSet is destroyed
- Includes Valgrind memory validation for debugging builds

## Simplified Source

```c
void LogicalTapeFreeze(LogicalTape *lt, TapeShare *share) {
    LogicalTapeSet *lts = lt->tapeSet;

    // Validation: must be in write mode
    Assert(lt->writing);
    Assert(lt->offsetBlockNumber == 0L);

    // Flush any pending write data
    if (lt->dirty) {
        // Set block size and write final block
        TapeBlockSetNBytes(lt->buffer, lt->nbytes);
        ltsWriteBlock(lt->tapeSet, lt->curBlockNumber, lt->buffer);
    }

    // Switch from write to frozen read mode
    lt->writing = false;
    lt->frozen = true;

    // Resize buffer to single block for optimal seeking
    if (!lt->buffer || lt->buffer_size != BLCKSZ) {
        if (lt->buffer)
            pfree(lt->buffer);
        lt->buffer = palloc(BLCKSZ);
        lt->buffer_size = BLCKSZ;
    }

    // Initialize read state - start from first block
    lt->curBlockNumber = lt->firstBlockNumber;
    lt->pos = 0;
    lt->nbytes = 0;

    // Handle empty tape case
    if (lt->firstBlockNumber == -1L) {
        lt->nextBlockNumber = -1L;
        return;
    }

    // Read first block and set up next block pointer
    ltsReadBlock(lt->tapeSet, lt->curBlockNumber, lt->buffer);
    if (TapeBlockIsLast(lt->buffer))
        lt->nextBlockNumber = -1L;
    else
        lt->nextBlockNumber = TapeBlockGetTrailer(lt->buffer)->next;
    lt->nbytes = TapeBlockGetNBytes(lt->buffer);

    // Set up sharing info for parallel sorts
    if (share) {
        BufFileExportFileSet(lts->pfile);
        share->firstblocknumber = lt->firstBlockNumber;
    }
}
```