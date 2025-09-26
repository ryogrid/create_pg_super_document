# cost_incremental_sort

## Location
[src/backend/optimizer/path/costsize.c:1986-2123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1986-L2123)

## Overview
Determines and returns the cost of sorting a relation incrementally when the input path is presorted by a prefix of the pathkeys.

## Definition

```c
void
cost_incremental_sort(Path *path,
					  PlannerInfo *root, List *pathkeys, int presorted_keys,
					  Cost input_startup_cost, Cost input_total_cost,
					  double input_tuples, int width, Cost comparison_cost, int sort_mem,
					  double limit_tuples)
```
## Detailed Description
This function calculates the cost of incremental sorting, which is an optimization for cases where input data is already sorted by some leading keys. Instead of sorting the entire dataset, it divides the input into groups based on the presorted keys and sorts each group individually using tuplesort.

The algorithm:
1. Estimates the number of groups formed by the leading presorted pathkeys
2. Calculates the average group size 
3. Uses cost_tuplesort() to estimate the cost of sorting each individual group
4. Adds overhead costs for group detection and tuplesort resets between groups

Special handling is implemented for expressions containing "varno 0" (introduced by generate_append_tlist), which would confuse estimate_num_groups - in such cases it defaults to DEFAULT_NUM_DISTINCT.

## Parameters / Member Variables
- : Output parameter - Path object to store the calculated costs and row estimates
- : PlannerInfo containing planner state and statistics
- : List of sort keys for the complete sort operation
- : Number of leading pathkeys by which input is already sorted
- : Startup cost of the input path
- : Total cost of the input path
- : Number of tuples from the input
- : Average tuple width in bytes
- : Extra cost per comparison operation
- : Amount of work memory available for sorting (in kilobytes)
- : Bound on output tuples; -1 if no limit

## Dependencies
- Functions called/Symbols referenced:
  - [cost_tuplesort](cost_tuplesort.md)
  - [estimate_num_groups](../e/estimate_num_groups.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [pull_varnos](../p/pull_varnos.md)
  - foreach_current_index
  - DEFAULT_NUM_DISTINCT
  - [PathKey](../P/PathKey.md)
  - [EquivalenceMember](../E/EquivalenceMember.md)
- Called from (representative examples):
  - [create_incremental_sort_path](create_incremental_sort_path.md)

## Notes and Other Information
- Ensures minimum tuple count of 2.0 to avoid zero cost estimates and log(0) calculations
- Default group estimate is capped at DEFAULT_NUM_DISTINCT and minimum of input_tuples
- Adds overhead costs: (cpu_tuple_cost + comparison_cost) per tuple for group detection
- Charges double cpu_tuple_cost per group for tuplesort_reset operations
- Defensive against "varno 0" expressions that could cause estimate_num_groups to fail
- Critical optimization for queries with partial ordering that can significantly reduce sort costs