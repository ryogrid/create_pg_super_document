# build_minmax_path

## Location
[src/backend/optimizer/plan/planagg.c:316-477](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planagg.c#L316-L477)

## Overview
Attempts to build an optimized index scan path for a single MIN/MAX aggregate by creating a specialized subquery with LIMIT 1.

## Definition

```c
struct what is effectively a sub-SELECT query, so
	 * clone the current query level's state and adjust it to make it look
	 * like a subquery.  Any outer references will now be one level higher
	 * than before.  (This means that when we are done, there will be no Vars
	 * of level 1, which is why the subquery can become an initplan.)
	 */
	subroot = (PlannerInfo *) palloc(sizeof(PlannerInfo));
```
## Detailed Description
This function constructs an optimized execution path for MIN/MAX aggregates by creating a modified subquery that can leverage index scans. The process involves:

1. **Subquery Construction**: Clones the current planner state and increments the query level to create an isolated subquery environment
2. **Query Transformation**: Converts the aggregate into a simple SELECT with:
   - Single target list entry for the aggregate column
   - IS NOT NULL condition on the target column
   - ORDER BY clause using the provided sort operator and null handling
   - LIMIT 1 to fetch only the minimum/maximum value
3. **Path Planning**: Invokes  with  to generate optimal paths
4. **Path Selection**: Chooses the cheapest fractional path for the required sort order
5. **Cost Calculation**: Computes the cost to retrieve just the first row from the sorted path

The function effectively transforms  into , allowing the optimizer to use index scans instead of full table scans.

## Parameters / Member Variables
- : PlannerInfo structure containing the current query planning context
- : MinMaxAggInfo structure that will be populated with the generated path information
- : OID of the equality operator corresponding to the sort operator
- : OID of the sort operator for ordering (ASC for MIN, DESC for MAX)
- : Boolean indicating whether NULL values should be sorted first

## Dependencies
- Functions called/Symbols referenced:
  -  - Main query planning function to generate paths
  -  - Callback function to customize query planning behavior
  -  - Selects optimal path for required ordering
  -  - Adjusts path to return correct target list
  -  - Handles parameter references in subquery
  -  - Adjusts costs for initialization plans
  -  - Adjusts variable reference levels for subquery
  -  - Creates sort group reference for ORDER BY clause
- Called from (representative examples):
  -  (src/backend/optimizer/plan/planagg.c:175, 177)

## Notes and Other Information
- Returns true if a suitable index path is found, false otherwise
- The function tries both NULLS FIRST and NULLS LAST orderings to find the best available index
- Generated subquery becomes an initplan since it has no level-1 variables after transformation
- [Path](../P/Path.md) costs are calculated to match  methodology
- Assumes the target expression was already validated as non-mutable and non-rowtype
- The IS NOT NULL condition is only added if not already present in the WHERE clause

## Simplified Source

```c
static bool
build_minmax_path(PlannerInfo *root, MinMaxAggInfo *mminfo,
                  Oid eqop, Oid sortop, bool nulls_first)
{
    PlannerInfo *subroot;
    Query *parse;
    TargetEntry *tle;
    List *tlist;
    NullTest *ntest;
    SortGroupClause *sortcl;
    RelOptInfo *final_rel;
    Path *sorted_path;
    Cost path_cost;
    double path_fraction;

    // Create subquery planning context
    subroot = (PlannerInfo *) palloc(sizeof(PlannerInfo));
    memcpy(subroot, root, sizeof(PlannerInfo));
    subroot->query_level++;
    subroot->parent_root = root;

    // Reset subplan-specific state
    subroot->plan_params = NIL;
    subroot->init_plans = NIL;

    // Clone and adjust query structure
    subroot->parse = parse = copyObject(root->parse);
    IncrementVarSublevelsUp((Node *) parse, 1, 1);

    // Build target list with single aggregate column
    tle = makeTargetEntry(copyObject(mminfo->target),
                          (AttrNumber) 1,
                          pstrdup("agg_target"),
                          false);
    tlist = list_make1(tle);
    subroot->processed_tlist = parse->targetList = tlist;

    // Remove HAVING, DISTINCT, aggregates from subquery
    parse->havingQual = NULL;
    parse->distinctClause = NIL;
    parse->hasAggs = false;

    // Add "target IS NOT NULL" condition if not already present
    ntest = makeNode(NullTest);
    ntest->nulltesttype = IS_NOT_NULL;
    ntest->arg = copyObject(mminfo->target);
    ntest->argisrow = false;

    if (!list_member((List *) parse->jointree->quals, ntest))
        parse->jointree->quals = (Node *)
            lcons(ntest, (List *) parse->jointree->quals);

    // Create ORDER BY clause for MIN/MAX optimization
    sortcl = makeNode(SortGroupClause);
    sortcl->tleSortGroupRef = assignSortGroupRef(tle, subroot->processed_tlist);
    sortcl->eqop = eqop;
    sortcl->sortop = sortop;
    sortcl->nulls_first = nulls_first;
    parse->sortClause = list_make1(sortcl);

    // Add LIMIT 1
    parse->limitCount = (Node *) makeConst(INT8OID, -1, InvalidOid,
                                           sizeof(int64),
                                           Int64GetDatum(1), false,
                                           FLOAT8PASSBYVAL);

    // Plan the optimized subquery
    subroot->tuple_fraction = 1.0;
    subroot->limit_tuples = 1.0;

    final_rel = query_planner(subroot, minmax_qp_callback, NULL);

    // Handle subquery parameters and costs
    SS_identify_outer_params(subroot);
    SS_charge_for_initplans(subroot, final_rel);

    // Find best path for retrieving just one row
    if (final_rel->rows > 1.0)
        path_fraction = 1.0 / final_rel->rows;
    else
        path_fraction = 1.0;

    sorted_path = get_cheapest_fractional_path_for_pathkeys(
        final_rel->pathlist,
        subroot->query_pathkeys,
        NULL,
        path_fraction);

    if (!sorted_path)
        return false;

    // Apply projection to get correct target list
    sorted_path = apply_projection_to_path(subroot, final_rel, sorted_path,
                                           create_pathtarget(subroot,
                                                             subroot->processed_tlist));

    // Calculate cost for first row only
    path_cost = sorted_path->startup_cost +
        path_fraction * (sorted_path->total_cost - sorted_path->startup_cost);

    // Save results in mminfo structure
    mminfo->subroot = subroot;
    mminfo->path = sorted_path;
    mminfo->pathcost = path_cost;

    return true;
}
```