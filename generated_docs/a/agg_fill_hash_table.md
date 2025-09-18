# agg_fill_hash_table

## Location
src/backend/executor/nodeAgg.c: 2540 - 2593

## Overview
agg_fill_hash_table builds hash tables for hashed aggregation by reading all input tuples from the outer plan and populating hash entries with aggregate state values.

## Definition
```c
static void
agg_fill_hash_table(AggState *aggstate)
```

## Detailed Description
agg_fill_hash_table implements the initial phase of hash-based aggregation in PostgreSQL. This function is responsible for consuming all input tuples from the outer plan and building the hash table(s) used for grouping and aggregation.

The function operates in a straightforward loop that:

1. **Input Processing**: Fetches tuples one by one from the outer plan using fetch_input_tuple until input is exhausted
2. **Hash Entry Management**: For each input tuple, calls lookup_hash_entries to find existing hash entries or create new ones based on the grouping key
3. **Aggregate Advancement**: Updates aggregate transition values by calling advance_aggregates for the current tuple
4. **Context Cleanup**: Resets the per-input-tuple expression context after processing each tuple

After processing all input tuples, the function:
- Finalizes any spilled hash table data using hashagg_finish_initial_spills
- Marks the table as filled (aggstate->table_filled = true)
- Initializes the hash table iterator for the first hash table to prepare for the retrieval phase

This function is called only once per aggregation operation when using AGG_HASHED or AGG_MIXED strategies, and only when the hash table hasn't been filled yet.

## Parameters / Member Variables
- `aggstate`: The AggState structure containing execution state, hash tables, and per-tuple expression context

## Dependencies
- Functions called/Symbols referenced:
  - fetch_input_tuple
  - TupIsNull
  - lookup_hash_entries
  - advance_aggregates
  - ResetExprContext
  - hashagg_finish_initial_spills
  - select_current_set
  - ResetTupleHashIterator
- Called from (representative examples):
  - ExecAgg (when using AGG_HASHED strategy and table not yet filled)

## Notes and Other Information
- This function only builds the hash table; retrieval is handled by agg_retrieve_hash_table
- The function handles spilling to disk automatically through hashagg_finish_initial_spills when memory limits are exceeded
- Multiple hash tables may be built for different grouping sets, but initialization focuses on the first one
- Expression context reset is performed both explicitly and implicitly through hash lookups
- The table_filled flag prevents redundant hash table building in subsequent calls