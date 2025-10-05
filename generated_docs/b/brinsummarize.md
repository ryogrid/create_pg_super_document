# brinsummarize

## Location
[src/backend/access/brin/brin.c:1878-1975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L1878-L1975)

## Overview
Summarizes page ranges in a BRIN index that are not already summarized, optionally processing the entire table or a specific page range.

## Definition
```c
static void brinsummarize(Relation index, Relation heapRel, BlockNumber pageRange, bool include_partial, double *numSummarized, double *numExisting)
```

## Detailed Description
This static function performs bulk summarization of BRIN index ranges by scanning the reverse map to identify missing summary tuples and creating them. It can operate in two modes: summarizing the entire table when pageRange is BRIN_ALL_BLOCKRANGES, or summarizing a specific page range. The function initializes build state and index info only when needed (lazy initialization), processes each range by checking for existing summary tuples, and calls summarize_range for ranges that need summarization. It optionally counts the number of ranges summarized versus those already existing.

## Parameters / Member Variables
- `index`: The BRIN index relation to summarize
- `heapRel`: The heap relation being indexed
- `pageRange`: Specific page range to summarize, or BRIN_ALL_BLOCKRANGES for entire table
- `include_partial`: Whether to include partial ranges at the end of the table
- `numSummarized`: Optional counter for newly summarized ranges (may be NULL)
- `numExisting`: Optional counter for already existing summary tuples (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [brinRevmapInitialize](brinRevmapInitialize.md)
  - RelationGetNumberOfBlocks
  - [brinRevmapTerminate](brinRevmapTerminate.md)
  - [brinGetTupleForHeapBlock](brinGetTupleForHeapBlock.md)
  - [initialize_brin_buildstate](../i/initialize_brin_buildstate.md)
  - [BuildIndexInfo](../B/BuildIndexInfo.md)
  - [summarize_range](../s/summarize_range.md)
  - [brin_memtuple_initialize](brin_memtuple_initialize.md)
  - [terminate_brin_buildstate](../t/terminate_brin_buildstate.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
- Constants referenced:
  - BRIN_ALL_BLOCKRANGES
  - BUFFER_LOCK_SHARE
  - BUFFER_LOCK_UNLOCK
  - InvalidBuffer
  - InvalidBlockNumber
- Types referenced:
  - [BrinRevmap](../B/BrinRevmap.md)
  - [BrinBuildState](../B/BrinBuildState.md)
  - [IndexInfo](../I/IndexInfo.md)
  - [BrinTuple](../B/BrinTuple.md)
  - BlockNumber
  - OffsetNumber
  - Buffer
- Called from (representative examples):
  - [brinvacuumcleanup](brinvacuumcleanup.md)
  - [brin_summarize_range](brin_summarize_range.md)

## Notes and Other Information
- This is a static function only accessible within the brin.c module
- Uses lazy initialization for build state and index info to optimize performance when no work is needed
- The include_partial parameter controls whether to process partial ranges at table end, typically true for bulk loading scenarios and false for maintenance operations
- Implements proper resource cleanup including buffer release and state termination
- Critical function for BRIN index maintenance and ensuring summary coverage of heap data
- Handles the case where start block is beyond table end by early return

## Simplified Source
```c
static void
brinsummarize(Relation index, Relation heapRel, BlockNumber pageRange,
              bool include_partial, double *numSummarized, double *numExisting)
{
    BrinRevmap *revmap;
    BrinBuildState *state = NULL;
    IndexInfo  *indexInfo = NULL;
    BlockNumber heapNumBlocks;
    BlockNumber pagesPerRange;
    Buffer      buf;
    BlockNumber startBlk;

    // Initialize reverse map to track existing summaries
    revmap = brinRevmapInitialize(index, &pagesPerRange);

    // Determine range of pages to process
    heapNumBlocks = RelationGetNumberOfBlocks(heapRel);
    if (pageRange == BRIN_ALL_BLOCKRANGES)
        startBlk = 0;  // Process entire table
    else
    {
        // Process specific range
        startBlk = (pageRange / pagesPerRange) * pagesPerRange;
        heapNumBlocks = Min(heapNumBlocks, startBlk + pagesPerRange);
    }

    if (startBlk > heapNumBlocks)
    {
        // Nothing to do if beyond table end
        brinRevmapTerminate(revmap);
        return;
    }

    // Scan each range and summarize missing ones
    buf = InvalidBuffer;
    for (; startBlk < heapNumBlocks; startBlk += pagesPerRange)
    {
        BrinTuple  *tup;
        OffsetNumber off;

        // Skip partial ranges if not requested
        if (!include_partial && (startBlk + pagesPerRange > heapNumBlocks))
            break;

        // Check if this range already has a summary
        tup = brinGetTupleForHeapBlock(revmap, startBlk, &buf, &off, NULL,
                                      BUFFER_LOCK_SHARE);
        if (tup == NULL)
        {
            // No summary exists - create one
            if (state == NULL)
            {
                // Lazy initialization on first use
                state = initialize_brin_buildstate(index, revmap,
                                                  pagesPerRange,
                                                  InvalidBlockNumber);
                indexInfo = BuildIndexInfo(index);
            }

            // Summarize this range
            summarize_range(indexInfo, state, heapRel, startBlk, heapNumBlocks);
            brin_memtuple_initialize(state->bs_dtuple, state->bs_bdesc);

            if (numSummarized)
                *numSummarized += 1.0;
        }
        else
        {
            // Summary already exists
            if (numExisting)
                *numExisting += 1.0;
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        }
    }

    // Cleanup resources
    if (BufferIsValid(buf))
        ReleaseBuffer(buf);
    brinRevmapTerminate(revmap);
    if (state)
    {
        terminate_brin_buildstate(state);
        pfree(indexInfo);
    }
}
```