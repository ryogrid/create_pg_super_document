# make_subplan

## Location
[src/backend/optimizer/plan/subselect.c:162-318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L162-L318)

## Overview
Converts a SubLink node (as created by the parser) into a SubPlan, handling the planning process for subqueries and determining whether to implement them as regular SubPlans or InitPlans.

## Definition
```c
static Node *make_subplan(PlannerInfo *root, Query *orig_subquery, SubLinkType subLinkType, int subLinkId, Node *testexpr, bool isTopQual)
```

## Detailed Description
This function is responsible for converting a SubLink node into executable form during query planning. It takes a subquery and determines the optimal execution strategy, creating either a SubPlan (for correlated subqueries) or an InitPlan (for uncorrelated subqueries that can be executed once).

The function handles special optimizations for EXISTS sublinks, including the ability to simplify them and potentially convert them to equivalent ANY sublinks that can be implemented using hash tables for better performance. When both execution strategies are viable, it creates an AlternativeSubPlan node, leaving the final choice to the execution phase.

Key operations include:
1. Copying the original subquery to avoid parser tree conflicts
2. Applying EXISTS-specific simplifications when applicable
3. Setting appropriate tuple fraction hints based on sublink type
4. Planning the subquery using the subquery planner
5. Converting the resulting plan to SubPlan or InitPlan format
6. Creating alternative execution plans when beneficial

## Parameters / Member Variables
- `root`: PlannerInfo context for the outer query
- `orig_subquery`: The original subquery to be planned
- `subLinkType`: Type of SubLink (EXISTS, ANY, ALL, EXPR, MULTIEXPR, ROWCOMPARE)
- `subLinkId`: Unique identifier for the SubLink
- `testexpr`: Test expression associated with the SubLink (already processed)
- `isTopQual`: Whether this expression appears at the top level of a WHERE/HAVING clause

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - [simplify_EXISTS_query](../s/simplify_EXISTS_query.md)
  - [subquery_planner](../s/subquery_planner.md)
  - [fetch_upper_rel](../f/fetch_upper_rel.md)
  - [get_cheapest_fractional_path](../g/get_cheapest_fractional_path.md)
  - [create_plan](../c/create_plan.md)
  - [build_subplan](../b/build_subplan.md)
  - [convert_EXISTS_to_ANY](../c/convert_EXISTS_to_ANY.md)
  - [subpath_is_hashable](../s/subpath_is_hashable.md)
  - makeNode
  - list_make2
- Called from (representative examples):
  - [process_sublinks_mutator](../p/process_sublinks_mutator.md)

## Notes and Other Information
- The function implements sophisticated optimization logic for EXISTS sublinks, potentially creating both a regular SubPlan and a hash-based ANY SubPlan alternative
- Tuple fraction hints are carefully chosen based on sublink type: 1.0 for EXISTS (like LIMIT 1), 0.5 for ALL/ANY (expecting early termination), and 0.0 for others
- The function handles the complexity of parameter passing between outer and inner queries
- Located in src/backend/optimizer/plan/subselect.c:162-318
- Returns either a SubPlan node, a Param node (for InitPlans), or an AlternativeSubPlan node containing multiple execution strategies

## Simplified Source

```c
static Node *make_subplan(PlannerInfo *root, Query *orig_subquery,
                         SubLinkType subLinkType, int subLinkId,
                         Node *testexpr, bool isTopQual) {
    Query *subquery;
    bool simple_exists = false;
    double tuple_fraction;
    PlannerInfo *subroot;
    RelOptInfo *final_rel;
    Path *best_path;
    Plan *plan;
    List *plan_params;
    Node *result;

    // Copy the subquery to avoid parser tree conflicts
    subquery = copyObject(orig_subquery);

    // Try to simplify EXISTS subqueries
    if (subLinkType == EXISTS_SUBLINK)
        simple_exists = simplify_EXISTS_query(root, subquery);

    // Set tuple fraction hints based on sublink type
    if (subLinkType == EXISTS_SUBLINK)
        tuple_fraction = 1.0;  // Like LIMIT 1
    else if (subLinkType == ALL_SUBLINK || subLinkType == ANY_SUBLINK)
        tuple_fraction = 0.5;  // Expect early termination
    else
        tuple_fraction = 0.0;  // Default behavior

    // Plan the subquery
    subroot = subquery_planner(root->glob, subquery, root, false, tuple_fraction, NULL);

    // Capture parameters needed by this subplan
    plan_params = root->plan_params;
    root->plan_params = NIL;

    // Select best path and create plan
    final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, NULL);
    best_path = get_cheapest_fractional_path(final_rel, tuple_fraction);
    plan = create_plan(subroot, best_path);

    // Convert to SubPlan or InitPlan format
    result = build_subplan(root, plan, best_path, subroot, plan_params,
                          subLinkType, subLinkId, testexpr, NIL, isTopQual);

    // For correlated EXISTS, try to create hash-based alternative
    if (simple_exists && IsA(result, SubPlan)) {
        // Create alternative ANY-based execution plan if beneficial
        Query *alt_subquery = copyObject(orig_subquery);
        Node *newtestexpr;
        List *paramIds;

        if (convert_EXISTS_to_ANY(root, alt_subquery, &newtestexpr, &paramIds)) {
            // Plan the ANY version and check if hashable
            // If so, create AlternativeSubPlan with both options
            // ... (hash planning logic)
        }
    }

    return result;
}
```