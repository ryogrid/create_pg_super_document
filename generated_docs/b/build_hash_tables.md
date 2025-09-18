# build_hash_tables

## Location
src/backend/executor/nodeAgg.c: 1468 - 1502

## Overview
Initializes or resets hash tables used for hashed aggregation, creating one hash table for each grouping set that requires hashing.

## Definition


## Detailed Description
The  function manages the lifecycle of hash tables used in PostgreSQL's hashed aggregation implementation. For each grouping set that requires hashing, it either resets an existing hash table to empty or creates a new one from scratch.

The function implements a multi-hash table approach where each grouping set gets its own dedicated hash table and associated data structures. This design allows PostgreSQL to efficiently handle queries with multiple GROUP BY clauses or complex grouping operations. Each hash table stores representative tuples along with arrays of AggStatePerGroup structures that maintain the aggregation state for each distinct combination of GROUP BY column values.

Memory management is carefully handled by distributing the available hash memory limit evenly across all hash tables. The function uses PostgreSQL's hash_choose_num_buckets utility to determine an optimal bucket count based on the expected number of groups, available memory, and hash entry size. All hash table contents reside in the hashcontext's per-tuple memory context, enabling efficient bulk resets.

## Parameters / Member Variables
- : The AggState execution node containing hash table configuration, memory limits, and per-hash data structures

## Dependencies
- Functions called/Symbols referenced:
  - ResetTupleHashTable
  - [hash_choose_num_buckets](../h/hash_choose_num_buckets.md)
  - [build_hash_table](build_hash_table.md)
- Types referenced:
  - [AggState](../A/AggState.md)
  - [AggStatePerHash](../A/AggStatePerHash.md)
- Called from (representative examples):
  - [ExecInitAgg](../E/ExecInitAgg.md)
  - [ExecReScanAgg](../E/ExecReScanAgg.md)

## Notes and Other Information
- Creates separate hash tables for each grouping set to handle complex GROUP BY operations efficiently
- Evenly distributes available hash memory across all hash tables to prevent memory exhaustion
- Uses hash_choose_num_buckets for optimal bucket sizing based on expected group cardinality
- All hash table data lives in a shared hashcontext for efficient memory management and bulk operations
- The hash_ngroups_current counter is reset to 0, tracking the current number of groups across all hash tables
- Existing hash tables are reset rather than destroyed and recreated, improving performance during rescans