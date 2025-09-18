# acquire_sample_rows

## Location
[src/backend/commands/analyze.c:1158-1314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L1158-L1314)

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
  - [GetOldestNonRemovableTransactionId](../G/GetOldestNonRemovableTransactionId.md)
  - BlockSampler_Init
  - [table_beginscan_analyze](../t/table_beginscan_analyze.md)
  - [table_scan_analyze_next_block](../t/table_scan_analyze_next_block.md)
  - [table_scan_analyze_next_tuple](../t/table_scan_analyze_next_tuple.md)
  - reservoir_init_selection_state
  - reservoir_get_next_S
  - [compare_rows](../c/compare_rows.md) (for sorting)
  - ExecCopySlotHeapTuple
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [analyze_rel](analyze_rel.md)
  - [acquire_inherited_sample_rows](acquire_inherited_sample_rows.md)

## Notes and Other Information
- Uses Vitter's reservoir sampling algorithm for statistically sound random sampling
- Implements block sampling to reduce I/O while maintaining statistical validity
- Not perfect - large relations may have too few different blocks represented in the sample
- Handles vacuum delays and progress reporting for long-running operations
- Returns tuples sorted by physical position for correlation analysis
- Provides detailed progress logging including pages scanned and row counts found