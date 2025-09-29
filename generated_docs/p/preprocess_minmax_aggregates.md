# preprocess_minmax_aggregates

## Location
[src/backend/optimizer/plan/planagg.c:72-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planagg.c#L72-L235)

## Overview
Preprocesses MIN/MAX aggregate functions to determine if they can be optimized via index scans, creating a MinMaxAggPath when optimization is possible.

## Definition

```c
void
preprocess_minmax_aggregates(PlannerInfo *root)
```
## Detailed Description
This function analyzes queries containing MIN/MAX aggregate functions to determine if they can be optimized using index scans instead of full table scans. It performs several validation checks to ensure the query structure is compatible with the optimization:

1. **Query Structure Validation**: Rejects queries with GROUP BY clauses, multiple grouping sets, window functions, CTEs, or complex joins
2. **Table Restrictions**: Only handles queries referencing exactly one table (including inheritance hierarchies and flattened UNION ALL subqueries)
3. **Aggregate Analysis**: Verifies all aggregates are MIN/MAX functions via 
4. **Index Path Building**: Attempts to build index paths for each aggregate using 
5. **Path Creation**: Creates a  node with estimated costs and adds it to the  upperrel for cost comparison

The optimization works by using index scans to directly fetch the minimum or maximum values without scanning all table rows, providing significant performance improvements for eligible queries.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and state information

## Dependencies
- Functions called/Symbols referenced:
  -  - Validates that all aggregates are MIN/MAX functions
  -  - Attempts to build index scan paths for each aggregate
  -  - Gets equality operator for aggregate's ordering operator
  -  - Creates output parameters for aggregates
  -  - Creates the MinMaxAggPath node
  -  - Retrieves the GROUP_AGG upperrel
  -  - Adds the path to the relation for cost comparison
- Called from (representative examples):
  -  (src/backend/optimizer/plan/planner.c:1517)

## Notes and Other Information
- Must be called after  since it relies on 
- Called just before  since it clones planner state for path generation  
- Creates PARAM_EXEC slots for each aggregate even if the optimization isn't ultimately used
- [MinMaxAggPath](../M/MinMaxAggPath.md) nodes are currently never parallel-safe
- The optimization is most effective for queries like  where  has suitable indexes

## Simplified Source

```c
void preprocess_minmax_aggregates(PlannerInfo *root)
{
    Query *parse = root->parse;
    List *aggs_list;
    RelOptInfo *grouped_rel;

    // Early exit if no aggregates present
    if (!parse->hasAggs)
        return;

    // Reject complex query structures that prevent optimization
    if (parse->groupClause ||
        list_length(parse->groupingSets) > 1 ||
        parse->hasWindowFuncs ||
        parse->cteList)
        return;

    // Ensure query references exactly one table
    FromExpr *jtnode = parse->jointree;
    while (IsA(jtnode, FromExpr)) {
        if (list_length(jtnode->fromlist) != 1)
            return;
        jtnode = linitial(jtnode->fromlist);
    }

    if (!IsA(jtnode, RangeTblRef))
        return;

    RangeTblRef *rtr = (RangeTblRef *) jtnode;
    RangeTblEntry *rte = planner_rt_fetch(rtr->rtindex, root);

    // Only handle regular relations or flattened UNION ALL
    if (!(rte->rtekind == RTE_RELATION ||
          (rte->rtekind == RTE_SUBQUERY && rte->inh)))
        return;

    // Verify all aggregates are MIN/MAX functions
    aggs_list = NIL;
    if (!can_minmax_aggs(root, &aggs_list))
        return;

    // Build index paths for each aggregate
    foreach(lc, aggs_list) {
        MinMaxAggInfo *mminfo = (MinMaxAggInfo *) lfirst(lc);
        Oid eqop;
        bool reverse;

        // Get equality operator for ordering
        eqop = get_equality_op_for_ordering_op(mminfo->aggsortop, &reverse);
        if (!OidIsValid(eqop))
            elog(ERROR, "could not find equality operator");

        // Try to build index path (both NULLS FIRST and NULLS LAST)
        if (build_minmax_path(root, mminfo, eqop, mminfo->aggsortop, reverse))
            continue;
        if (build_minmax_path(root, mminfo, eqop, mminfo->aggsortop, !reverse))
            continue;

        // If no index path found, optimization fails
        return;
    }

    // Create output parameters for each aggregate
    foreach(lc, aggs_list) {
        MinMaxAggInfo *mminfo = (MinMaxAggInfo *) lfirst(lc);
        mminfo->param = SS_make_initplan_output_param(root,
                                                     exprType((Node *) mminfo->target),
                                                     -1,
                                                     exprCollation((Node *) mminfo->target));
    }

    // Create and add MinMaxAggPath to compete with standard aggregation
    grouped_rel = fetch_upper_rel(root, UPPERREL_GROUP_AGG, NULL);
    add_path(grouped_rel, (Path *)
        create_minmaxagg_path(root, grouped_rel,
                             create_pathtarget(root, root->processed_tlist),
                             aggs_list,
                             (List *) parse->havingQual));
}
```