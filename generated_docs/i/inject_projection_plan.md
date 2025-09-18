# inject_projection_plan

## Location
[src/backend/optimizer/plan/createplan.c:2121-2152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2121-L2152)

## Overview
Inserts a Result node to perform projection on-the-fly when a projection step is needed during plan tree construction.

## Definition


## Detailed Description
The  function is a utility that creates a Result node to perform projection when it's determined during plan construction that a projection step is required. This function is used as a "fallback" mechanism when the planner realizes on-the-fly that projection is needed, rather than having it planned from the beginning during path creation.

The function is considered somewhat "ugly" in the codebase because it represents a deviation from the clean separation between path planning and plan creation phases. Ideally, projection requirements should be determined during path creation rather than being injected during plan construction.

Key characteristics:
- **Simple Result node creation**: Creates a Result node with the specified target list and no qualification conditions
- **Cost preservation**: Copies the cost estimates from the subplan rather than adding projection costs, as the projection costs were likely not accounted for during path construction
- **Parallel safety**: Accepts an explicit parallel_safe parameter since the target list expressions might be parallel-unsafe even if the subplan is parallel-safe

## Parameters / Member Variables
- : The child plan node that will provide input tuples
- : The target list (list of expressions) to be computed by the Result node
- : Boolean indicating whether the projection is safe for parallel execution

## Dependencies
- Functions called/Symbols referenced:
  - [make_result](../m/make_result.md)
  - [copy_plan_costsize](../c/copy_plan_costsize.md)
- Called from (representative examples):
  - [create_append_plan](../c/create_append_plan.md)
  - [create_merge_append_plan](../c/create_merge_append_plan.md)
  - [change_plan_targetlist](../c/change_plan_targetlist.md)
  - [prepare_sort_from_pathkeys](../p/prepare_sort_from_pathkeys.md)

## Notes and Other Information
- This function is acknowledged in the code comments as a design compromise that should ideally be eliminated in favor of more systematic projection planning
- The cost model is simplified - it doesn't add the actual cost of computing the target list expressions, instead copying the subplan's costs to maintain consistency in EXPLAIN output
- Used in various contexts where plan nodes need to be adapted to produce different target lists than originally planned
- The parallel_safe parameter must be carefully set based on analysis of the target list expressions, as parallel-unsafe expressions cannot be safely executed in parallel workers
- Common usage scenarios include Append/MergeAppend plans where child plans need unified target lists, and sort preparation where additional expressions need to be computed