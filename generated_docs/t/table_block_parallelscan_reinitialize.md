# table_block_parallelscan_reinitialize

## Location
src/backend/access/table/tableam.c: 407 - 421

## Overview
Resets the parallel scan descriptor to allow restarting a parallel scan of a relation from the beginning.

## Definition


## Detailed Description
This function reinitializes an existing parallel scan descriptor to reset it for a fresh scan of the same relation. The primary purpose is to reset the allocation counter that tracks how many blocks have been distributed to worker processes. This allows the same parallel scan descriptor to be reused for multiple scan passes over the same relation.

The function performs a minimal reset operation - it only resets the  atomic counter to zero. Other fields like the total number of blocks, relation ID, synchronization settings, and mutex remain unchanged since they are still valid for the same relation.

This is more efficient than completely reinitializing the descriptor, as it preserves the existing configuration while resetting only the state that needs to be cleared between scan passes.

## Parameters / Member Variables
- : Relation being scanned (currently unused but maintained for API consistency)
- : Generic parallel table scan descriptor (cast to ParallelBlockTableScanDesc internally)

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_write_u64
  - ParallelTableScanDesc (type cast)
  - ParallelBlockTableScanDesc (target type)
- Called from (representative examples):
  - SampleHeapTupleVisible
  - table_scan_sample_next_tuple

## Notes and Other Information
- Part of the parallel scan infrastructure for block-oriented access methods
- Much simpler than full initialization - only resets the allocated blocks counter
- The relation parameter is currently unused but kept for API consistency and potential future extensions
- Uses atomic write operation to ensure thread-safety when resetting the counter
- Allows efficient reuse of parallel scan descriptors for multiple passes over the same data
- Does not reset other fields like phs_nblocks, phs_syncscan, or phs_startblock which remain valid for the same relation