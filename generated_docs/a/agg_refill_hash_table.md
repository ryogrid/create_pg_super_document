# agg_refill_hash_table

## Location
src/backend/executor/nodeAgg.c: 2594 - 2745

## Overview
agg_refill_hash_table reprocesses spilled hash aggregation batches by reading spilled tuples from disk, rebuilding the hash table, and handling memory overflow during the refill process.

## Definition
```c
static bool
agg_refill_hash_table(AggState *aggstate)
```

## Detailed Description
agg_refill_hash_table is a critical function for handling memory overflow in hash aggregation. When the hash table exceeds available memory (hash_mem), PostgreSQL spills groups to disk in batches. This function reprocesses those spilled batches one at a time, allowing hash aggregation to complete even with limited memory.

**Batch Processing Logic**:
1. **Batch Selection**: Retrieves the next batch from aggstate->hash_batches (processed as a stack, LIFO order)
2. **Memory Management**: Sets appropriate memory limits using hash_agg_set_limits based on batch characteristics
3. **Hash Table Reset**: Clears existing hash tables and resets group counters to prepare for the new batch
4. **Phase Management**: For AGG_MIXED mode, switches to phase 1 during batch processing, then back to phase 0

**Spill Processing Loop**:
- Reads MinimalTuples from the batch's input tape using hashagg_batch_read
- Attempts to insert each tuple into the hash table
- If memory is available, creates new hash entries or updates existing ones
- If memory is exhausted, re-spills tuples to new batches for later processing

**Expression Recompilation**: Since spilled tuples are stored as MinimalTuples (different format from outer plan), the function recompiles aggregate expressions using hashagg_recompile_expressions.

**Completion Handling**: After processing all tuples in a batch, the function finalizes any new spills, updates metrics, and prepares the hash table iterator for retrieval.

## Parameters / Member Variables
- `aggstate`: The AggState structure containing spill batches, hash tables, and execution state

## Dependencies
- Functions called/Symbols referenced:
  - llast
  - list_delete_last
  - hash_agg_set_limits
  - MemSet
  - ReScanExprContext
  - ResetTupleHashTable
  - select_current_set
  - hashagg_recompile_expressions
  - hashagg_batch_read
  - ExecStoreMinimalTuple
  - prepare_hash_slot
  - LookupTupleHashEntryHash
  - initialize_hash_entry
  - advance_aggregates
  - hashagg_spill_init
  - hashagg_spill_tuple
  - ResetExprContext
  - LogicalTapeClose
  - hashagg_spill_finish
  - hash_agg_update_metrics
  - ResetTupleHashIterator
- Called from (representative examples):
  - agg_retrieve_hash_table (when processing spilled batches)

## Notes and Other Information
- Returns false when no more batches exist (aggstate->hash_batches == NIL), true otherwise
- The function processes only one grouping set per batch, setting others to NULL
- Spill initialization is deferred until actually needed to avoid allocating unused tapes
- Handles recursive spilling when memory is still insufficient during batch reprocessing
- Uses hash values stored with spilled tuples to avoid recomputing hash codes
- Critical for PostgreSQL's ability to handle hash aggregation larger than available memory
- The batch processing order (LIFO) helps with memory locality and efficient processing