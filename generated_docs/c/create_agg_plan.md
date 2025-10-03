# create_agg_plan

## Location
[src/backend/optimizer/plan/createplan.c:2309-2354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2309-L2354)

## Overview
Creates an Agg (aggregation) plan node for the given AggPath, including recursive creation of plans for its subpaths.

## Definition

```c
static Agg *
create_agg_plan(PlannerInfo *root, AggPath *best_path)
```
## Detailed Description
The  function is responsible for creating an Agg plan node from an AggPath structure. This function handles the construction of aggregation plans which are fundamental for implementing SQL GROUP BY operations, aggregate functions (COUNT, SUM, etc.), and HAVING clauses. The function recursively creates the subplan using , builds the target list for the aggregation, processes qualification clauses, and extracts grouping information to construct the final Agg plan node.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state and context information
- `*best_path`: AggPath structure representing the chosen aggregation path with all necessary aggregation details
## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md)
  - [build_path_tlist](../b/build_path_tlist.md)
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - [make_agg](../m/make_agg.md)
  - [extract_grouping_cols](../e/extract_grouping_cols.md)
  - [extract_grouping_ops](../e/extract_grouping_ops.md)
  - [extract_grouping_collations](../e/extract_grouping_collations.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- The function is static, indicating it's only used within the createplan.c file
- [Agg](../A/Agg.md) plans can project, so the function doesn't need to be strict about the child target list, but grouping columns must be available
- The function extracts grouping information including columns, operators, and collations from the AggPath
- Uses CP_LABEL_TLIST flag when creating the subplan to ensure proper target list labeling
- The created plan includes information about aggregation strategy, split mode, number of groups, and transition space requirements

## Simplified Source

```c
static Agg *
create_agg_plan(PlannerInfo *root, AggPath *best_path)
{
    // Create the subplan recursively
    Plan *subplan = create_plan_recurse(root, best_path->subpath, CP_LABEL_TLIST);

    // Build target list for aggregation
    List *tlist = build_path_tlist(root, &best_path->path);

    // Process qualification clauses
    List *quals = order_qual_clauses(root, best_path->qual);

    // Create the aggregation plan node
    Agg *plan = make_agg(
        tlist, quals,
        best_path->aggstrategy,
        best_path->aggsplit,
        list_length(best_path->groupClause),
        extract_grouping_cols(best_path->groupClause, subplan->targetlist),
        extract_grouping_ops(best_path->groupClause),
        extract_grouping_collations(best_path->groupClause, subplan->targetlist),
        NIL, NIL,
        best_path->numGroups,
        best_path->transitionSpace,
        subplan
    );

    // Copy generic path information to plan
    copy_generic_path_info(&plan->plan, (Path *) best_path);

    return plan;
}
```