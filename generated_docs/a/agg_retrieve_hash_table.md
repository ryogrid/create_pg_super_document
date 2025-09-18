# agg_retrieve_hash_table

## Location
src/backend/executor/nodeAgg.c: 2746 - 2770

## Overview
agg_retrieve_hash_table manages the retrieval phase of hash aggregation by coordinating between in-memory hash table retrieval and processing of spilled batches until all aggregated groups are returned.

## Definition
```c
static TupleTableSlot *
agg_retrieve_hash_table(AggState *aggstate)
```

## Detailed Description
agg_retrieve_hash_table serves as the main coordinator for the retrieval phase of hash-based aggregation. It implements a two-tier retrieval strategy that handles both in-memory and spilled data seamlessly:

**Primary Retrieval Strategy**:
1. **In-Memory Retrieval**: First attempts to retrieve results from the current in-memory hash table using agg_retrieve_hash_table_in_memory
2. **Spill Processing**: When in-memory results are exhausted, calls agg_refill_hash_table to process the next batch of spilled data
3. **Completion Detection**: Marks aggregation as complete (agg_done = true) when both in-memory and spilled data are exhausted

**Coordination Logic**: The function operates in a loop that continues until a valid result tuple is found or all data is exhausted. This design handles the complex interaction between:
- Multiple hash tables (for different grouping sets)
- Spilled batches that need to be reprocessed
- Memory management during batch processing

**Memory Management Integration**: By coordinating with agg_refill_hash_table, this function enables PostgreSQL to handle hash aggregations that exceed available memory (hash_mem). The spill-and-refill mechanism allows processing of arbitrarily large datasets without memory exhaustion.

The function abstracts the complexity of spill handling from the executor, providing a simple interface that returns one aggregated group at a time, regardless of whether the data comes from memory or disk.

## Parameters / Member Variables
- `aggstate`: The AggState structure containing hash tables, spill batches, and execution state

## Dependencies
- Functions called/Symbols referenced:
  - agg_retrieve_hash_table_in_memory
  - agg_refill_hash_table
- Called from (representative examples):
  - ExecAgg (for AGG_HASHED and AGG_MIXED strategies)
  - agg_retrieve_direct (when switching to hash mode in AGG_MIXED)

## Notes and Other Information
- This function provides a unified interface for hash table retrieval regardless of spill status
- The loop continues until either a result is found or all data sources are exhausted
- Essential for PostgreSQL's ability to handle large hash aggregations that exceed memory limits
- Works seamlessly with grouping sets by delegating grouping set management to the underlying functions
- The function sets agg_done only when truly complete, ensuring proper cleanup and state management
- Critical component in PostgreSQL's spill-to-disk strategy for memory-constrained hash aggregation