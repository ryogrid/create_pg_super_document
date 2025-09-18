# cost_agg

## Location
src/backend/optimizer/path/costsize.c: 2650 - 2853

## Overview
Calculates the startup and total costs for performing an Agg plan node, considering different aggregation strategies (plain, sorted, hashed, mixed) and accounting for spilling to disk when memory limits are exceeded.

## Definition
void cost_agg(Path *path, PlannerInfo *root, AggStrategy aggstrategy, const AggClauseCosts *aggcosts, int numGroupCols, double numGroups, List *quals, Cost input_startup_cost, Cost input_total_cost, double input_tuples, double input_width)

## Detailed Description
The cost_agg function estimates the cost of executing an Agg node in PostgreSQL's query planner. Agg nodes perform aggregation operations like SUM, COUNT, AVG and handle grouping operations. The function supports multiple aggregation strategies:

- **AGG_PLAIN**: Single-group aggregation without grouping columns
- **AGG_SORTED**: Grouped aggregation on pre-sorted input, delivering output on-the-fly
- **AGG_HASHED**: Hash-based grouping that processes all input before producing output
- **AGG_MIXED**: Hybrid approach that may fall back to sorting if hashing is disabled

The costing model accounts for:
- Transition function costs (per input tuple) and finalization costs (per output group)
- Grouping comparison costs for sorted aggregation
- Hash computation and retrieval costs for hashed aggregation
- Disk spilling costs when hash aggregation exceeds memory limits
- HAVING clause evaluation costs and their selectivity impact

For hash aggregation spilling, the function performs sophisticated analysis using hash_agg_entry_size and hash_agg_set_limits to estimate memory usage, number of batches, and I/O costs including read/write penalties.

## Parameters / Member Variables
- : Path node to store the calculated costs and output row count
- : PlannerInfo structure containing planner context and aggregate transition info
- : Aggregation strategy (AGG_PLAIN, AGG_SORTED, AGG_HASHED, or AGG_MIXED)
- : Structure containing per-aggregate cost information, can be NULL for grouping-only operations
- : Number of columns used for grouping
- : Estimated number of output groups
- : List of HAVING clause expressions to evaluate
- : Startup cost from the input path
- : Total cost from the input path
- : Number of input tuples
- : Average width in bytes of input tuples

## Dependencies
- Functions called/Symbols referenced:
  - [hash_agg_entry_size](../h/hash_agg_entry_size.md) (estimates memory per hash table entry)
  - [hash_agg_set_limits](../h/hash_agg_set_limits.md) (calculates memory and group count limits)
  - [relation_byte_size](../r/relation_byte_size.md) (calculates total bytes for tuples)
  - [cost_qual_eval](cost_qual_eval.md) (evaluates HAVING clause costs)
  - [clamp_row_est](clamp_row_est.md) (ensures row estimates are within reasonable bounds)
  - [clauselist_selectivity](clauselist_selectivity.md) (calculates selectivity of HAVING clauses)
  - AggStrategy, AggClauseCosts (data types for aggregation parameters)
- Called from (representative examples):
  - [create_agg_path](create_agg_path.md) (in pathnode.c:3206)
  - [create_groupingsets_path](create_groupingsets_path.md) (in pathnode.c:3312, 3337, 3362)
  - [create_unique_path](create_unique_path.md) (in pathnode.c:1828)

## Notes and Other Information
- AGG_SORTED and AGG_HASHED are designed to have identical total CPU costs with different startup costs
- Uses dummy_aggcosts when aggcosts is NULL (typically for grouping-only hash aggregation)
- For hash aggregation spilling: applies 2x penalty for I/O operations due to typical hardware/OS behavior
- Spill cost calculation considers recursive partitioning depth and includes CPU costs for tuple spilling/reading
- Requires appropriately-sorted input when aggstrategy is AGG_SORTED
- Adds disable_cost penalty when enable_hashagg is false for hash-based strategies
- Output tuple count is set to 1 for AGG_PLAIN, numGroups for other strategies (adjusted by HAVING selectivity)