# find_cols

## Location
src/backend/executor/nodeAgg.c: 1397 - 1419

## Overview
Finds input-tuple columns that are needed for aggregation processing, dividing them into aggregated and unaggregated sets.

## Definition


## Detailed Description
The  function analyzes the target list and qualification conditions of an aggregation plan to identify which input columns are referenced. It categorizes these column references into two distinct sets: those that appear within aggregate function expressions (aggregated) and those that appear outside of aggregate functions (unaggregated). This classification is essential for optimization decisions in aggregation processing, particularly for determining which columns need to be available during different phases of aggregation.

The function uses a tree walker approach to traverse the plan's target list and qualification expressions, employing a context structure to track whether column references are found within aggregate function calls or not. Additionally, it ensures that grouping columns specified in the aggregation plan are included in the unaggregated set, even if they don't explicitly appear in the target list.

## Parameters / Member Variables
- : The AggState execution state node containing the aggregation plan and runtime information
- : Output parameter - pointer to a Bitmapset that will contain column numbers referenced within aggregate functions
- : Output parameter - pointer to a Bitmapset that will contain column numbers referenced outside of aggregate functions

## Dependencies
- Functions called/Symbols referenced:
  - find_cols_walker
  - bms_add_member
- Types referenced:
  - AggState
  - Agg
  - FindColsContext
  - Bitmapset
- Called from (representative examples):
  - find_hash_columns

## Notes and Other Information
- This function is part of the aggregation optimization infrastructure in PostgreSQL's executor
- The function operates on plans that have already been processed by setrefs.c, ensuring that variable references use OUTER_VAR
- Grouping columns are automatically included in the unaggregated set regardless of their appearance in the target list or quals
- The resulting bitmapsets are used by calling functions to make decisions about column projection and hash table construction during aggregation