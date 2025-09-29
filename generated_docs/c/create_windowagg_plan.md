# create_windowagg_plan

## Location
[src/backend/optimizer/plan/createplan.c:2617-2719](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2617-L2719)

## Overview
Creates a WindowAgg plan node for WindowAggPath operations that implement SQL window functions like ROW_NUMBER(), RANK(), and aggregate functions with OVER clauses.

## Definition
```c
static WindowAgg *create_windowagg_plan(PlannerInfo *root, WindowAggPath *best_path)
```

## Detailed Description
The `create_windowagg_plan` function constructs a WindowAgg plan node that implements SQL window functions. It processes the window specification (PARTITION BY and ORDER BY clauses) from the WindowClause, converts SortGroupClause lists into arrays of column indexes, equality operators, and collations needed by the executor. The function creates arrays for both partition columns (which define window boundaries) and ordering columns (which determine row sequence within partitions). It uses CP_SMALL_TLIST flag when creating the subplan to minimize memory usage since WindowAgg stores input rows in a tuplestore for window frame processing.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context information
- `best_path`: WindowAggPath structure containing the window clause, run conditions, qualifications, and subpath information

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md)
  - [build_path_tlist](../b/build_path_tlist.md)
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md)
  - [exprCollation](../e/exprCollation.md)
  - [make_windowagg](../m/make_windowagg.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- The function is static, used only within createplan.c
- Uses CP_SMALL_TLIST flag to request a compact target list since WindowAgg stores input rows in tuplestores
- Converts PARTITION BY and ORDER BY clauses into executor-friendly arrays of column indexes, operators, and collations
- Handles window frame specifications including frame options, start/end offsets, and range functions
- Supports complex window frame definitions with ROWS, RANGE, and GROUPS modes
- The created plan includes run conditions for optimized window function execution and qualifications for filtering
- Essential for implementing SQL:2003 window function standards including analytical functions and frame-aware aggregates

## Simplified Source

```c
static WindowAgg *
create_windowagg_plan(PlannerInfo *root, WindowAggPath *best_path)
{
    WindowAgg *plan;
    WindowClause *wc = best_path->winclause;
    Plan *subplan;
    List *tlist;

    // Create subplan with small target list for tuplestore efficiency
    subplan = create_plan_recurse(root, best_path->subpath,
                                  CP_LABEL_TLIST | CP_SMALL_TLIST);

    // Build target list for window operations
    tlist = build_path_tlist(root, &best_path->path);

    // Convert PARTITION BY clauses to executor arrays
    AttrNumber *partColIdx = build_partition_columns(wc->partitionClause, subplan);
    Oid *partOperators = build_partition_operators(wc->partitionClause);
    Oid *partCollations = build_partition_collations(wc->partitionClause, subplan);

    // Convert ORDER BY clauses to executor arrays
    AttrNumber *ordColIdx = build_order_columns(wc->orderClause, subplan);
    Oid *ordOperators = build_order_operators(wc->orderClause);
    Oid *ordCollations = build_order_collations(wc->orderClause, subplan);

    // Create WindowAgg plan node
    plan = make_windowagg(tlist, wc->winref,
                          partNumCols, partColIdx, partOperators, partCollations,
                          ordNumCols, ordColIdx, ordOperators, ordCollations,
                          wc->frameOptions, wc->startOffset, wc->endOffset,
                          wc->startInRangeFunc, wc->endInRangeFunc,
                          wc->inRangeColl, wc->inRangeAsc, wc->inRangeNullsFirst,
                          best_path->runCondition, best_path->qual,
                          best_path->topwindow, subplan);

    copy_generic_path_info(&plan->plan, (Path *) best_path);
    return plan;
}
```