# hash_agg_update_metrics

## Location
src/backend/executor/nodeAgg.c: 1917 - 1965

## Overview
Updates memory usage metrics and performance statistics for hash aggregation operations, tracking peak memory consumption, disk usage, and hash entry size estimates.

## Definition


## Detailed Description
This function updates various metrics related to hash aggregation performance after filling the hash table. It calculates and tracks memory usage across different components including the hash table metadata, group keys and transition states, and tape buffer memory when spilling occurs. The function also updates peak memory usage, disk usage when tapes are involved, and provides updated estimates for hash entry size based on current group count.

The function only operates when the aggregation strategy is either AGG_MIXED or AGG_HASHED, returning early for other strategies.

## Parameters / Member Variables
- : The AggState structure containing the hash aggregation state and metrics
- : Boolean indicating whether data is being read from tape (true) or from the outer plan (false)
- : Number of partitions used in the hash aggregation, affects buffer memory calculation

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextMemAllocated
  - LogicalTapeSetBlocks
  - HASHAGG_WRITE_BUFFER_SIZE
  - HASHAGG_READ_BUFFER_SIZE
  - TupleHashEntryData
  - AggState
  - AGG_MIXED
  - AGG_HASHED
- Called from (representative examples):
  - agg_refill_hash_table
  - hashagg_finish_initial_spills

## Notes and Other Information
- The function tracks three main types of memory: metadata context memory, hashkey context memory, and buffer memory for tape operations
- Peak memory tracking helps PostgreSQL make informed decisions about when to spill to disk
- Hash entry size estimation is dynamically updated based on actual memory usage per group, improving future planning
- Buffer memory calculation includes write buffers for all partitions and optionally a read buffer when reading from tape
- Disk usage is measured in kilobytes and only tracked when a tape set exists