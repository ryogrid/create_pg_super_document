# tuplesort_gettuple_common

## Location
src/backend/utils/sort/tuplesort.c: 1496 - 1735

## Overview
The core internal function that fetches the next tuple in either forward or backward direction during the tuple sorting process, handling different sorting states and memory management strategies.

## Definition


## Detailed Description
This is the central tuple retrieval function in PostgreSQL's tuplesort implementation that abstracts the complexity of fetching tuples from different storage contexts. The function handles three distinct sorting states:

1. **TSS_SORTEDINMEM**: When all tuples fit in memory and are sorted in-place in the memtuples array
2. **TSS_SORTEDONTAPE**: When tuples are stored on a single logical tape after being sorted
3. **TSS_FINALMERGE**: During the final merge phase when multiple sorted runs are being merged

The function implements bidirectional tuple access for random access sorts and manages memory through a slab allocator system. It handles EOF conditions, bounded sorts validation, and complex tape positioning for backward scans. The returned tuple belongs to the tuplesort memory context and may be recycled on subsequent calls.

## Parameters / Member Variables
- : The Tuplesortstate containing all sort context including current position, memory management, and tape references
- : Boolean indicating scan direction - true for forward, false for backward (requires TUPLESORT_RANDOMACCESS)
- : Output parameter where the retrieved SortTuple is stored

## Dependencies
- Functions called/Symbols referenced:
  - WORKER (macro to check worker process state)
  - RELEASE_SLAB_SLOT (memory management for slab allocator)
  - getlen (reads tuple length from logical tape)
  - READTUP (reads tuple data from tape)
  - LogicalTapeBackspace (positions tape backward)
  - LogicalTapeClose (closes logical tape)
  - mergereadnext (reads next tuple during merge)
  - tuplesort_heap_delete_top (heap management during merge)
  - tuplesort_heap_replace_top (heap management during merge)
- Called from (representative examples):
  - tuplesort_skiptuples
  - tuplesort_gettupleslot
  - tuplesort_getheaptuple
  - tuplesort_getindextuple
  - tuplesort_getbrintuple
  - tuplesort_getdatum

## Notes and Other Information
- The function enforces that backward scanning requires TUPLESORT_RANDOMACCESS option
- Memory from returned tuples may be recycled on subsequent calls, requiring careful handling by callers
- Backward scanning on tapes involves complex positioning logic to read tuple length headers
- The function validates bounded sort limits to prevent over-fetching
- During final merge, it maintains a heap of the current front tuples from each input run
- The slab allocator is used for memory management when tuples don't fit entirely in memory