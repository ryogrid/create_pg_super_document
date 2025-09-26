# hash_agg_entry_size

## Location
[src/backend/executor/nodeAgg.c:1694-1740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1694-L1740)

## Overview
Estimates the per-hash-table-entry memory overhead for hash aggregation operations by calculating the total size needed for a single hash table entry including tuple data, per-group state, and transition space.

## Definition
```c
Size hash_agg_entry_size(int numTrans, Size tupleWidth, Size transitionSpace)
```

## Detailed Description
This function calculates the memory footprint of a single entry in a hash aggregation table. It accounts for all memory components required for storing and processing aggregation data:

1. **Tuple Storage**: Allocates space for the minimal tuple header plus the actual tuple width
2. **Per-Group State**: Allocates memory for aggregate state data structures (one per transition function)  
3. **Transition Space**: Allocates additional space needed by aggregate transition functions for intermediate computations

The function uses PostgreSQL's memory chunk system (CHUNKHDRSZ) to account for memory management overhead. Each component that requires memory gets its own chunk with proper alignment using MAXALIGN.

## Parameters / Member Variables
- `numTrans`: Number of aggregate transition functions in the aggregation operation
- `tupleWidth`: Size in bytes of the tuple data (excluding header)
- `transitionSpace`: Additional memory space required by transition functions for intermediate results

## Dependencies
- Functions called/Symbols referenced:
  - SizeofMinimalTupleHeader
  - [AggStatePerGroupData](../A/AggStatePerGroupData.md)
  - CHUNKHDRSZ
  - [TupleHashEntryData](../T/TupleHashEntryData.md)
- Called from (representative examples):
  - [ExecInitAgg](../E/ExecInitAgg.md)
  - [cost_agg](../c/cost_agg.md)
  - [estimate_hashagg_tablesize](../e/estimate_hashagg_tablesize.md)

## Notes and Other Information
- This function is critical for memory planning in hash aggregation operations
- The calculation helps the query planner decide whether to use hash-based or sort-based aggregation
- Memory chunks are used to manage the different components separately, allowing for efficient memory allocation and deallocation
- The function accounts for alignment requirements through MAXALIGN to ensure proper memory access performance