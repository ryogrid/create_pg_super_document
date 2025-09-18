# brinsummarize

## Location
src/backend/access/brin/brin.c: 1878 - 1975

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
  - ReleaseBuffer
- Constants referenced:
  - BRIN_ALL_BLOCKRANGES
  - BUFFER_LOCK_SHARE
  - BUFFER_LOCK_UNLOCK
  - InvalidBuffer
  - InvalidBlockNumber
- Types referenced:
  - [BrinRevmap](../B/BrinRevmap.md)
  - [BrinBuildState](../B/BrinBuildState.md)
  - IndexInfo
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