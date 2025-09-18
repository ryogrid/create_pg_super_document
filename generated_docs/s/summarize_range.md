# summarize_range

## Location
[src/backend/access/brin/brin.c:1752-1877](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L1752-L1877)

## Overview
Summarizes a heap page range for a BRIN index by scanning the corresponding heap blocks and creating or updating the summary tuple, handling concurrent insertions through placeholder tuple mechanisms.

## Definition
```c
static void summarize_range(IndexInfo *indexInfo, BrinBuildState *state, Relation heapRel, BlockNumber heapBlk, BlockNumber heapNumBlks)
```

## Detailed Description
This static function performs the core BRIN summarization operation for a specific block range. It implements a sophisticated concurrency control mechanism by first inserting a placeholder tuple, then scanning the heap range, and finally updating the placeholder with the summarized values. The function handles concurrent insertions by repeatedly attempting to update the summary tuple until successful, union-ing any concurrent modifications with the scanned values. It also handles the corner case of partial ranges at the end of the table by recomputing the table size after placeholder insertion.

## Parameters / Member Variables
- `indexInfo`: Index information structure containing metadata about the index being built
- `state`: BrinBuildState containing the current build state and configuration
- `heapRel`: The heap relation being summarized
- `heapBlk`: Starting block number of the range to summarize (must be aligned to range boundaries)
- `heapNumBlks`: Total number of blocks in the heap relation (may be outdated)

## Dependencies
- Functions called/Symbols referenced:
  - [brin_form_placeholder_tuple](../b/brin_form_placeholder_tuple.md)
  - [brin_doinsert](../b/brin_doinsert.md)
  - RelationGetNumberOfBlocks
  - [table_index_build_range_scan](../t/table_index_build_range_scan.md)
  - [brinbuildCallback](../b/brinbuildCallback.md)
  - [brin_form_tuple](../b/brin_form_tuple.md)
  - [brin_can_do_samepage_update](../b/brin_can_do_samepage_update.md)
  - [brin_doupdate](../b/brin_doupdate.md)
  - [brin_free_tuple](../b/brin_free_tuple.md)
  - [brinGetTupleForHeapBlock](../b/brinGetTupleForHeapBlock.md)
  - [brin_copy_tuple](../b/brin_copy_tuple.md)
  - [union_tuples](../u/union_tuples.md)
  - ReleaseBuffer
- Types referenced:
  - IndexInfo
  - [BrinBuildState](../B/BrinBuildState.md)
  - [BrinTuple](../B/BrinTuple.md)
  - BlockNumber
  - Buffer
  - Size
  - OffsetNumber
- Called from (representative examples):
  - [brinsummarize](../b/brinsummarize.md)

## Notes and Other Information
- This is a static function only accessible within the brin.c module
- Uses placeholder tuples to handle concurrent insertions during range scanning
- The function uses "any visible" mode for heap scanning to capture tuples from in-progress transactions
- Implements retry logic to handle concurrent updates to placeholder tuples
- Handles partial ranges at table end by recalculating table size after placeholder insertion
- Critical for maintaining BRIN index accuracy in multi-user environments
- The range start block must be aligned to pagesPerRange boundaries (asserted in code)