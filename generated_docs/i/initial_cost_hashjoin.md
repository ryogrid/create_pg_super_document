initial_cost_hashjoin

## Overview
Produces preliminary lower-bound cost estimates for a hash join path, focusing on I/O and hash table construction costs while deferring detailed CPU cost analysis.

## Definition
```c
void initial_cost_hashjoin(PlannerInfo *root, JoinCostWorkspace *workspace,
                          JoinType jointype, List *hashclauses,
                          Path *outer_path, Path *inner_path,
                          JoinPathExtraData *extra, bool parallel_hash)
```

## Detailed Description
This function provides a fast preliminary cost estimate for hash join operations, designed to quickly eliminate obviously expensive paths from consideration. It focuses on the major cost components that can be estimated without detailed analysis of join qualification clauses.

The function calculates startup costs including source data costs and hash table construction, and run costs for probing the hash table. It uses ExecChooseHashTableSize to determine the optimal hash table configuration (buckets, batches, skew optimization) and accounts for the additional I/O costs when the inner relation is too large to fit in memory and requires batching.

For parallel hash joins, it adjusts the inner relation row count to account for the total rows across all parallel workers when estimating hash table size.

The division of labor with final_cost_hashjoin represents a speed vs. accuracy tradeoff - this function provides fast lower bounds while final_cost_hashjoin performs the detailed CPU cost analysis.

## Parameters / Member Variables
- `root`: PlannerInfo containing query planning context
- `workspace`: JoinCostWorkspace to be filled with preliminary cost estimates
- `jointype`: Type of join operation being performed
- `hashclauses`: List of join clauses to be used for hashing
- `outer_path`: Path for the outer (probing) relation  
- `inner_path`: Path for the inner (hash table) relation
- `extra`: JoinPathExtraData with miscellaneous join information
- `parallel_hash`: Whether this will use a shared parallel hash table

## Dependencies
- Functions called/Symbols referenced:
  - [get_parallel_divisor](../g/get_parallel_divisor.md)
  - ExecChooseHashTableSize
  - [page_size](../p/page_size.md)
- Called from (representative examples):
  - [try_hashjoin_path](../t/try_hashjoin_path.md)
  - [try_partial_hashjoin_path](../t/try_partial_hashjoin_path.md)

## Notes and Other Information
- Assumes skew optimization will always be performed for simplicity
- Charges cpu_operator_cost per hash clause per tuple plus cpu_tuple_cost for hash table insertion
- For batched joins, adds sequential I/O costs: inner pages at startup, inner + 2*outer pages at runtime
- Saves intermediate results (numbuckets, numbatches, run_cost) in workspace for final_cost_hashjoin
- Does not examine join qualification clauses in detail, deferring CPU qualification costs to final costing
- For parallel hash, undoes the parallel divisor when estimating total hash table size