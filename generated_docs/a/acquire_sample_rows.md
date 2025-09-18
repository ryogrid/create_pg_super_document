# acquire_sample_rows

## Location
src/backend/commands/analyze.c: 1158 - 1314

## Overview
Acquires a random sample of rows from a table using a two-stage reservoir sampling algorithm, providing statistically unbiased estimates of live and dead row counts for table analysis.

## Definition


## Detailed Description
The acquire_sample_rows function implements a sophisticated two-stage random sampling method for PostgreSQL's ANALYZE command. Stage one uses block sampling to select up to targrows random blocks (or all blocks if there aren't many). Stage two applies the Vitter reservoir sampling algorithm to create a random sample of targrows rows from the selected blocks.

The function processes blocks and rows simultaneously - each block is analyzed as soon as stage one selects it, while stage two controls which tuples are inserted into the sample reservoir. This approach ensures that every row has an equal chance of being selected while maintaining statistical validity for estimating table statistics.

The returned tuples are sorted by physical position (ItemPointer) to enable correlation estimates later in the analysis process. The algorithm provides unbiased estimates of average live and dead rows per block, addressing limitations of previous sampling methods that overweighted data near the start of tables.

## Parameters / Member Variables
- : The relation (table) to sample from
- : Error reporting level for progress messages
- : Caller-allocated array to store sampled tuples (must have at least targrows entries)
- : Target number of rows to sample
- : Output parameter for estimated total live rows in the table
- : Output parameter for estimated total dead rows in the table

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfBlocks
  - GetOldestNonRemovableTransactionId
  - BlockSampler_Init
  - table_beginscan_analyze
  - table_scan_analyze_next_block
  - table_scan_analyze_next_tuple
  - reservoir_init_selection_state
  - reservoir_get_next_S
  - compare_rows (for sorting)
  - ExecCopySlotHeapTuple
  - heap_freetuple
- Called from (representative examples):
  - analyze_rel
  - acquire_inherited_sample_rows

## Notes and Other Information
- Uses Vitter's reservoir sampling algorithm for statistically sound random sampling
- Implements block sampling to reduce I/O while maintaining statistical validity
- Not perfect - large relations may have too few different blocks represented in the sample
- Handles vacuum delays and progress reporting for long-running operations
- Returns tuples sorted by physical position for correlation analysis
- Provides detailed progress logging including pages scanned and row counts found