# add_partial_path_precheck

## Location
[src/backend/optimizer/util/pathnode.c:865-926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L865-L926)

## Overview
Performs a preliminary check to determine whether a proposed partial path is worth considering before performing the full add_partial_path evaluation.

## Definition

```c
bool
add_partial_path_precheck(RelOptInfo *parent_rel, Cost total_cost,
						  List *pathkeys)
```
## Detailed Description
The  function serves as an optimization to avoid expensive path creation and evaluation when a proposed partial path is clearly inferior to existing alternatives. It performs a lightweight comparison against existing partial paths and non-parallel plans to determine if a path is worth pursuing.

The function implements a two-phase evaluation:
1. **Partial Path Comparison**: Compares against existing partial paths in the partial_pathlist, focusing only on total cost and pathkeys (ignoring startup cost and parameterization)
2. **Non-Parallel Path Comparison**: Uses add_path_precheck to ensure the path could be competitive even against non-parallel alternatives

Unlike add_path_precheck, this function always compares pathkeys since partial_pathlist is expected to be short, making the comparison cost negligible while providing definitive answers.

## Parameters / Member Variables
- : The RelOptInfo structure representing the relation being analyzed
- : The total cost estimate for the proposed partial path
- : The pathkeys (sort order) that the proposed partial path would provide

## Dependencies
- Functions called/Symbols referenced:
  - [compare_pathkeys](../c/compare_pathkeys.md)
  - [add_path_precheck](add_path_precheck.md)
  - [PathKeysComparison](../P/PathKeysComparison.md) (enum)
  - PATHKEYS_DIFFERENT, PATHKEYS_BETTER1, PATHKEYS_BETTER2 (constants)
  - STD_FUZZ_FACTOR (constant)
  - Cost (type)

- Called from (representative examples):
  - [try_partial_nestloop_path](../t/try_partial_nestloop_path.md)
  - [try_partial_mergejoin_path](../t/try_partial_mergejoin_path.md) 
  - [try_partial_hashjoin_path](../t/try_partial_hashjoin_path.md)

## Notes and Other Information
- Returns true if the path should be considered further, false if it should be rejected immediately
- The function passes total_cost twice to add_path_precheck because startup cost is irrelevant for partial paths that will run to completion
- Uses fuzzy cost comparison (STD_FUZZ_FACTOR) to avoid rejecting paths with very similar costs
- Designed to work efficiently with short partial_pathlist, making pathkey comparison always worthwhile
- Helps avoid the overhead of creating Path nodes for obviously inferior alternatives