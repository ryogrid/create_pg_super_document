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
- `*path`: Output parameter - Path object to store the calculated costs and row estimates
- `*root`: PlannerInfo containing planner state and statistics
- `*pathkeys`: List of sort keys for the complete sort operation
- `presorted_keys`: Number of leading pathkeys by which input is already sorted
- `input_startup_cost`: Startup cost of the input path
- `input_total_cost`: Total cost of the input path
- `input_tuples`: Number of tuples from the input
- `width`: Average tuple width in bytes
- `comparison_cost`: Extra cost per comparison operation
- `sort_mem`: Amount of work memory available for sorting (in kilobytes)
- `limit_tuples`: Bound on output tuples; -1 if no limit
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

## Simplified Source

```c
void
cost_incremental_sort(Path *path, PlannerInfo *root, List *pathkeys, int presorted_keys,
                      Cost input_startup_cost, Cost input_total_cost,
                      double input_tuples, int width, Cost comparison_cost, int sort_mem,
                      double limit_tuples)
{
    Cost startup_cost, run_cost;
    Cost input_run_cost = input_total_cost - input_startup_cost;

    // Ensure minimum tuple count to avoid zero costs and log(0)
    if (input_tuples < 2.0)
        input_tuples = 2.0;

    // Estimate number of groups based on presorted keys
    double input_groups = Min(input_tuples, DEFAULT_NUM_DISTINCT);

    // Extract presorted key expressions, handling "varno 0" case
    List *presortedExprs = NIL;
    bool unknown_varno = false;

    foreach(l, pathkeys)
    {
        PathKey *key = (PathKey *) lfirst(l);
        EquivalenceMember *member = (EquivalenceMember *)
            linitial(key->pk_eclass->ec_members);

        // Check for problematic "varno 0" expressions
        if (bms_is_member(0, pull_varnos(root, (Node *) member->em_expr)))
        {
            unknown_varno = true;
            break;
        }

        presortedExprs = lappend(presortedExprs, member->em_expr);

        if (foreach_current_index(l) + 1 >= presorted_keys)
            break;
    }

    // Get better group estimate if expressions are safe
    if (!unknown_varno)
        input_groups = estimate_num_groups(root, presortedExprs, input_tuples,
                                          NULL, NULL);

    // Calculate per-group metrics
    double group_tuples = input_tuples / input_groups;
    Cost group_input_run_cost = input_run_cost / input_groups;

    // Cost to sort one group
    Cost group_startup_cost, group_run_cost;
    cost_tuplesort(&group_startup_cost, &group_run_cost,
                   group_tuples, width, comparison_cost, sort_mem,
                   limit_tuples);

    // Startup cost: first group setup + input cost
    startup_cost = group_startup_cost + input_startup_cost + group_input_run_cost;

    // Runtime cost: all groups + remaining input
    run_cost = group_run_cost +
               (group_run_cost + group_startup_cost) * (input_groups - 1) +
               group_input_run_cost * (input_groups - 1);

    // Add incremental sort overhead
    run_cost += (cpu_tuple_cost + comparison_cost) * input_tuples;  // Group detection
    run_cost += 2.0 * cpu_tuple_cost * input_groups;                // Reset overhead

    // Set final path costs
    path->rows = input_tuples;
    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
}
```