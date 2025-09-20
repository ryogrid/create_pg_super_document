# table_block_parallelscan_estimate

## Location
[src/backend/access/table/tableam.c:383-388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/tableam.c#L383-L388)

## Overview
Helper function that estimates the size of memory needed for parallel scan descriptor data in block-oriented access methods.

## Definition

```c
Size
table_block_parallelscan_estimate(Relation rel)
```
## Detailed Description
This function provides a memory size estimate for parallel scan operations on block-oriented access methods (AMs). It's part of the helper functions designed to implement parallel scans for block-oriented storage engines like heap tables. The function simply returns the size of the  structure, which contains the shared state needed to coordinate parallel scanning across multiple worker processes.

This function is typically called during the planning phase of parallel query execution to allocate appropriate shared memory space for the parallel scan descriptor.

## Parameters / Member Variables
- : Relation for which to estimate parallel scan memory requirements (currently unused in the implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelBlockTableScanDescData](../P/ParallelBlockTableScanDescData.md) (struct type)
  - sizeof operator
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)
  - table_scan_sample_next_tuple

## Notes and Other Information
- Part of the parallel scan infrastructure for block-oriented access methods
- The function currently ignores the relation parameter and returns a fixed size
- Returns a Size type (typically size_t) representing bytes needed
- This is a simple helper function that enables proper memory allocation for parallel scan coordination
- The returned size corresponds to the shared memory space needed to coordinate multiple worker processes scanning the same relation in parallel