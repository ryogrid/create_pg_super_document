# label_sort_with_costsize

## Location
[src/backend/optimizer/plan/createplan.c:5447-5478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5447-L5478)

## Overview
A utility function that estimates and labels the cost of a Sort plan node when it doesn't have a directly corresponding Path node, primarily used for EXPLAIN output.

## Definition

```c
static void
label_sort_with_costsize(PlannerInfo *root, Sort *plan, double limit_tuples)
```
## Detailed Description
This function is used in PostgreSQL's query planner to retroactively calculate and assign cost estimates to Sort plan nodes that were created without corresponding Path nodes. The function uses the cost_sort() function to estimate sorting costs based on the left subtree's characteristics and then assigns these costs to the Sort plan node. This is particularly important for providing accurate cost information in EXPLAIN output, even when the Sort node was created through plan manipulation rather than direct path-to-plan conversion.

The function specifically handles Sort nodes (not IncrementalSort nodes) and calculates costs based on the input from the left subtree, including total cost, number of rows, and row width.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state and configuration
- `*plan`: The Sort plan node to be labeled with cost information
- `limit_tuples`: Tuple limit for the sort operation (pass -1 if no limit), used by cost_sort for more accurate estimation
## Dependencies
- Functions called/Symbols referenced:
  - [cost_sort](../c/cost_sort.md) (to calculate sorting costs)
  - [Sort](../S/Sort.md) (plan node type being processed)
- Called from (representative examples):
  - [create_append_plan](../c/create_append_plan.md)
  - [create_merge_append_plan](../c/create_merge_append_plan.md)
  - [create_unique_plan](../c/create_unique_plan.md)
  - [create_mergejoin_plan](../c/create_mergejoin_plan.md)

## Notes and Other Information
- This is a static function within createplan.c, indicating it's for internal use within that module
- The function includes an assertion to ensure it only processes Sort nodes, not IncrementalSort nodes
- The cost calculation considers work_mem settings for memory-based sorting operations
- The function preserves parallel safety characteristics from the input plan
- Used primarily for labeling purposes to provide accurate cost information for EXPLAIN queries

## Simplified Source

```c
static void
label_sort_with_costsize(PlannerInfo *root, Sort *plan, double limit_tuples)
{
    Plan *lefttree = plan->plan.lefttree;
    Path sort_path;  // dummy for cost_sort result

    // Ensure we're dealing with a Sort node, not IncrementalSort
    Assert(IsA(plan, Sort));

    // Calculate sorting costs based on left subtree characteristics
    cost_sort(&sort_path, root, NIL,
              lefttree->total_cost,
              lefttree->plan_rows,
              lefttree->plan_width,
              0.0, work_mem, limit_tuples);

    // Apply calculated costs to the Sort plan node
    plan->plan.startup_cost = sort_path.startup_cost;
    plan->plan.total_cost = sort_path.total_cost;
    plan->plan.plan_rows = lefttree->plan_rows;
    plan->plan.plan_width = lefttree->plan_width;
    plan->plan.parallel_aware = false;
    plan->plan.parallel_safe = lefttree->parallel_safe;
}
```