# hashagg_spill_finish

## Location
src/backend/executor/nodeAgg.c: 3093 - 3132

## Overview
Transforms spilled hash aggregation partitions into new HashAggBatch structures for subsequent processing iterations.

## Definition
```c
static void hashagg_spill_finish(AggState *aggstate, HashAggSpill *spill, int setno)
```

## Detailed Description
This function processes spilled partitions from hash aggregation by converting each non-empty partition into a new HashAggBatch. When PostgreSQL's hash aggregation exceeds memory limits, tuples are spilled to disk in partitions. This function processes these spilled partitions by:

1. Iterating through each partition in the spill structure
2. Skipping empty partitions to avoid unnecessary work
3. Estimating cardinality using HyperLogLog sketches and then freeing the sketch memory
4. Preparing the logical tape for reading by rewinding it with an appropriate buffer size
5. Creating new HashAggBatch structures with the partition data
6. Adding these batches to the aggregation state's batch list for later processing
7. Cleaning up the spill partition memory structures

## Parameters / Member Variables
- `aggstate`: The aggregate execution state that will receive the new batches
- `spill`: The HashAggSpill structure containing partitioned spilled data
- `setno`: The grouping set number this spill belongs to

## Dependencies
- Functions called/Symbols referenced:
  - estimateHyperLogLog
  - freeHyperLogLog
  - LogicalTapeRewindForRead
  - [hashagg_batch_new](hashagg_batch_new.md)
  - lappend
  - [pfree](../p/pfree.md)
- Types used:
  - [AggState](../A/AggState.md)
  - [HashAggSpill](../H/HashAggSpill.md)
  - [LogicalTape](../L/LogicalTape.md)
  - [HashAggBatch](../H/HashAggBatch.md)
- Constants used:
  - HASHAGG_READ_BUFFER_SIZE
- Called from (representative examples):
  - [hashagg_finish_initial_spills](hashagg_finish_initial_spills.md) (src/backend/executor/nodeAgg.c:3071)
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md) (src/backend/executor/nodeAgg.c:2720)

## Notes and Other Information
- This is a static function internal to nodeAgg.c
- Returns early if there are no partitions to process (npartitions == 0)
- Uses HyperLogLog cardinality estimation to provide statistics for the new batches
- The `used_bits` calculation (32 - spill->shift) determines hash precision for the batch
- Memory cleanup includes freeing ntuples array, hll_card array, and partitions array
- Each processed partition becomes a separate batch in the aggstate->hash_batches list
- Increments hash_batches_used counter to track active batch count
- Part of PostgreSQL's disk-based hash aggregation strategy for handling large datasets