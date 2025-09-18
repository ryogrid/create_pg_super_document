# hashagg_finish_initial_spills

## Location
src/backend/executor/nodeAgg.c: 3059 - 3092

## Overview
Converts spilled hash aggregation partitions into new processing batches after the initial hash table processing is complete.

## Definition


## Detailed Description
This function is called after processing a HashAggBatch to handle any tuples that were spilled to disk during hash aggregation. When hash tables exceed memory limits, PostgreSQL spills some tuples to disk in partitions. This function processes those spilled partitions by converting them into new HashAggBatch structures that will be processed in subsequent iterations.

The function iterates through all grouping sets, processes each spill structure, and then cleans up the initial spill infrastructure since the system transitions from processing tuples from the outer plan to processing only batched spilled tuples. It also updates aggregation metrics and exits spill mode.

## Parameters / Member Variables
- : The aggregate execution state containing hash tables, spill information, and processing context

## Dependencies
- Functions called/Symbols referenced:
  - hashagg_spill_finish
  - hash_agg_update_metrics
  - pfree
- Types used:
  - AggState
  - HashAggSpill
- Called from (representative examples):
  - agg_retrieve_direct (src/backend/executor/nodeAgg.c:2467)
  - agg_fill_hash_table (src/backend/executor/nodeAgg.c:2572)

## Notes and Other Information
- This is a static function internal to nodeAgg.c
- Only processes spills if aggstate->hash_spills is not NULL
- After processing, it frees the hash_spills array and sets it to NULL
- Transitions the aggregation state out of spill mode by setting hash_spill_mode to false
- Updates metrics with the total number of partitions processed across all grouping sets
- This function is part of PostgreSQL's memory-constrained hash aggregation strategy that handles datasets larger than available memory