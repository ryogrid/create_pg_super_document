# get_memoize_path

## Location
[src/backend/optimizer/path/joinpath.c:581-720](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L581-L720)

## Overview
Creates a Memoize path node to cache results of parameterized inner scans in nested loop joins, improving performance when the same parameter values are repeated across multiple outer tuples.

## Definition

```c
static Path *
get_memoize_path(PlannerInfo *root, RelOptInfo *innerrel,
				 RelOptInfo *outerrel, Path *inner_path,
				 Path *outer_path, JoinType jointype,
				 JoinPathExtraData *extra)
```
## Detailed Description
This function determines whether it's beneficial to add a Memoize node above an inner path in a nested loop join. The Memoize node caches the results of inner scans based on parameter values, avoiding repeated execution when the same parameters are encountered from different outer tuples.

The function performs extensive validation to ensure memoization is safe and beneficial:
1. Verifies that memoization is enabled and the outer relation has sufficient rows to benefit from caching
2. Ensures there are cacheable parameters (either from parameterized clauses or lateral variables)
3. Handles special cases for SEMI/ANTI joins which require inner_unique to work correctly
4. Validates that unique joins have complete parameterization to ensure cache entry completeness
5. Checks for volatile functions that would make caching unsafe
6. Verifies that all parameter types have appropriate hash functions and equality operators

When all conditions are met, the function creates a MemoizePath using the collected parameter expressions and hash operators.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer context
- : RelOptInfo for the inner relation to be cached  
- : RelOptInfo for the outer relation providing parameters
- : Path representing the inner side of the join to be potentially cached
- : Path representing the outer side of the join
- : JoinType specifying the type of join operation
- : JoinPathExtraData containing additional join information including inner_unique flag

## Dependencies
- Functions called/Symbols referenced:
  - [paraminfo_get_equal_hashops](../p/paraminfo_get_equal_hashops.md)
  - [create_memoize_path](../c/create_memoize_path.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [bms_num_members](../b/bms_num_members.md)
  - JOIN_SEMI
  - JOIN_ANTI
- Called from (representative examples):
  - [match_unsorted_outer](../m/match_unsorted_outer.md)
  - [consider_parallel_nestloop](../c/consider_parallel_nestloop.md)

## Notes and Other Information
This function is static and used internally within joinpath.c. It implements sophisticated logic to handle unique joins where nested loops may not scan the inner relation to completion, requiring special handling for cache entry marking.

The function includes a detailed analysis of when memoization scope is limited for unique joins, noting that incomplete parameterization can prevent proper cache management. It also handles partitioned tables by considering top_parent relations when determining hash operations.

The memoization optimization is particularly effective for star-schema queries where dimension tables are repeatedly accessed with the same parameter values from fact table scans.