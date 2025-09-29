# create_setop_plan

## Location
[src/backend/optimizer/plan/createplan.c:2720-2755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2720-L2755)

## Overview
Creates a SetOp plan node for set operations (UNION, INTERSECT, EXCEPT) based on the given SetOpPath and recursively creates plans for its subpaths.

## Definition

```c
static SetOp *
create_setop_plan(PlannerInfo *root, SetOpPath *best_path, int flags)
```
## Detailed Description
This function is responsible for constructing a SetOp plan node that implements set operations in PostgreSQL's query execution. It takes a SetOpPath (which represents the chosen execution strategy for a set operation) and converts it into an executable plan. The function handles the creation of the subplan through recursive calls and properly configures the SetOp node with the necessary parameters for execution, including operation type, strategy, grouping information, and cardinality estimates.

The function ensures that grouping columns are properly labeled by passing the CP_LABEL_TLIST flag to the recursive plan creation, which is essential for set operations to correctly identify and compare tuples.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and information
- : SetOpPath representing the chosen execution path for the set operation, containing operation details like command type, strategy, and cardinality estimates  
- : Integer flags controlling plan creation behavior, modified to include CP_LABEL_TLIST for proper column labeling

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md)
  - [clamp_cardinality_to_long](clamp_cardinality_to_long.md)  
  - [make_setop](../m/make_setop.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - CP_LABEL_TLIST (flag constant)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- The function is static, indicating it's only used within the createplan.c module
- [SetOp](../S/SetOp.md) operations don't project new columns, so target list requirements pass through from parent operations
- Uses clamp_cardinality_to_long to safely convert cardinality estimates from double to long to prevent overflow
- The CP_LABEL_TLIST flag ensures grouping columns are properly labeled, which is crucial for set operation execution
- Part of PostgreSQL's query planner infrastructure for handling set operations like UNION, INTERSECT, and EXCEPT

## Simplified Source

```c
static SetOp *
create_setop_plan(PlannerInfo *root, SetOpPath *best_path, int flags)
{
    SetOp *plan;
    Plan *subplan;
    long numGroups;

    // Create subplan with labeled target list for grouping
    subplan = create_plan_recurse(root, best_path->subpath,
                                  flags | CP_LABEL_TLIST);

    // Convert cardinality estimate to safe long value
    numGroups = clamp_cardinality_to_long(best_path->numGroups);

    // Create the SetOp plan node
    plan = make_setop(best_path->cmd,
                      best_path->strategy,
                      subplan,
                      best_path->distinctList,
                      best_path->flagColIdx,
                      best_path->firstFlag,
                      numGroups);

    // Copy cost and path information
    copy_generic_path_info(&plan->plan, (Path *) best_path);

    return plan;
}
```