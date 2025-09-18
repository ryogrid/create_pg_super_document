# subplan_is_hashable

## Location
src/backend/optimizer/plan/subselect.c: 712 - 735

## Overview
Determines whether an ANY subplan can be implemented using hash-based execution by checking if the subquery result size fits within available hash memory limits.

## Definition
```c
static bool subplan_is_hashable(Plan *plan)
```

## Detailed Description
The `subplan_is_hashable` function evaluates whether a subquery can be executed using a hash-based approach for ANY subplans. This optimization allows PostgreSQL to build a hash table from the subquery results and perform lookups instead of repeatedly executing the subquery for each outer row.

The function focuses solely on the memory requirements of the subquery itself and does not evaluate whether the combining test expression is suitable for hashing (that check is performed elsewhere). It calculates the estimated memory footprint of the subquery result by multiplying the estimated number of rows by the estimated size per row, including tuple header overhead.

The memory estimation uses heap tuple overhead even though the actual storage uses MinimalTuples, providing a conservative estimate with built-in fudge factor for hashtable overhead. If the estimated size exceeds the available hash memory limit (controlled by the hash_mem configuration parameter), the function returns false, indicating that hash-based execution is not feasible.

## Parameters / Member Variables
- `plan`: The execution plan node representing the subquery whose hashability is being evaluated

## Dependencies
- Functions called/Symbols referenced:
  - `get_hash_memory_limit` (returns the hash_mem limit)
  - `SizeofHeapTupleHeader` (constant for tuple header size)
  - `MAXALIGN` (macro for memory alignment)
- Called from (representative examples):
  - `build_subplan` (src/backend/optimizer/plan/subselect.c:518)

## Notes and Other Information
This function is part of PostgreSQL's subplan optimization strategy for ANY/IN subqueries. Hash-based execution can provide significant performance improvements when the subquery result is small enough to fit in memory, as it eliminates the need for repeated subquery execution. The memory calculation includes safety margins through the use of heap tuple overhead and alignment, ensuring the actual memory usage doesn't exceed limits. This check is performed during query planning to decide between different subplan execution strategies.