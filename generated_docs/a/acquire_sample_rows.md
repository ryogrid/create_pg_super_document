# acquire_sample_rows

## Location
[src/backend/commands/analyze.c:1158-1314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L1158-L1314)

## Overview
Acquires a random sample of rows from a table using a two-stage reservoir sampling algorithm, providing statistically unbiased estimates of live and dead row counts for table analysis.

## Definition

```c
static int
acquire_sample_rows(Relation onerel, int elevel,
					HeapTuple *rows, int targrows,
					double *totalrows, double *totaldeadrows)
```
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
  - [BlockSampler_Init](../B/BlockSampler_Init.md)
  - [table_beginscan_analyze](../t/table_beginscan_analyze.md)
  - [table_scan_analyze_next_block](../t/table_scan_analyze_next_block.md)
  - [table_scan_analyze_next_tuple](../t/table_scan_analyze_next_tuple.md)
  - [reservoir_init_selection_state](../r/reservoir_init_selection_state.md)
  - [reservoir_get_next_S](../r/reservoir_get_next_S.md)
  - [compare_rows](../c/compare_rows.md) (for sorting)
  - [ExecCopySlotHeapTuple](../E/ExecCopySlotHeapTuple.md)
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

## Simplified Source

```c
static int
acquire_sample_rows(Relation onerel, int elevel,
                    HeapTuple *rows, int targrows,
                    double *totalrows, double *totaldeadrows)
{
    int numrows = 0;           // rows currently in sample
    double samplerows = 0;     // total rows processed
    double liverows = 0;       // live rows encountered
    double deadrows = 0;       // dead rows encountered
    double rowstoskip = -1;    // reservoir sampling state

    // Initialize sampling structures
    BlockNumber totalblocks = RelationGetNumberOfBlocks(onerel);
    TransactionId OldestXmin = GetOldestNonRemovableTransactionId(onerel);

    // Setup block sampler and reservoir sampler
    uint32 randseed = pg_prng_uint32(&pg_global_prng_state);
    BlockSamplerData bs;
    BlockNumber nblocks = BlockSampler_Init(&bs, totalblocks, targrows, randseed);

    ReservoirStateData rstate;
    reservoir_init_selection_state(&rstate, targrows);

    // Setup table scan
    TableScanDesc scan = table_beginscan_analyze(onerel);
    TupleTableSlot *slot = table_slot_create(onerel, NULL);
    ReadStream *stream = read_stream_begin_relation(READ_STREAM_MAINTENANCE,
                                                   vac_strategy, scan->rs_rd,
                                                   MAIN_FORKNUM,
                                                   block_sampling_read_stream_next,
                                                   &bs, 0);

    // Two-stage sampling: blocks then tuples within blocks
    while (table_scan_analyze_next_block(scan, stream)) {
        vacuum_delay_point();

        while (table_scan_analyze_next_tuple(scan, OldestXmin,
                                           &liverows, &deadrows, slot)) {
            // Reservoir sampling algorithm
            if (numrows < targrows) {
                // Fill initial reservoir
                rows[numrows++] = ExecCopySlotHeapTuple(slot);
            } else {
                // Replace random element using Vitter's algorithm
                if (rowstoskip < 0)
                    rowstoskip = reservoir_get_next_S(&rstate, samplerows, targrows);

                if (rowstoskip <= 0) {
                    int k = (int) (targrows * sampler_random_fract(&rstate.randstate));
                    heap_freetuple(rows[k]);
                    rows[k] = ExecCopySlotHeapTuple(slot);
                }
                rowstoskip -= 1;
            }
            samplerows += 1;
        }

        // Update progress reporting
        pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_DONE, ++blksdone);
    }

    // Cleanup scan resources
    read_stream_end(stream);
    ExecDropSingleTupleTableSlot(slot);
    table_endscan(scan);

    // Sort tuples by physical position for correlation analysis
    if (numrows == targrows)
        qsort_interruptible(rows, numrows, sizeof(HeapTuple), compare_rows, NULL);

    // Extrapolate total row counts from sample
    if (bs.m > 0) {
        *totalrows = floor((liverows / bs.m) * totalblocks + 0.5);
        *totaldeadrows = floor((deadrows / bs.m) * totalblocks + 0.5);
    } else {
        *totalrows = 0.0;
        *totaldeadrows = 0.0;
    }

    // Log sampling results
    ereport(elevel, (errmsg("\"%s\": scanned %d of %u pages, "
                           "containing %.0f live rows and %.0f dead rows; "
                           "%d rows in sample, %.0f estimated total rows",
                           RelationGetRelationName(onerel),
                           bs.m, totalblocks, liverows, deadrows,
                           numrows, *totalrows)));

    return numrows;
}
```