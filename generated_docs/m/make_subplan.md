# make_subplan

## Location
src/backend/optimizer/plan/subselect.c: 162 - 318

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
  - simplify_EXISTS_query
  - subquery_planner
  - fetch_upper_rel
  - get_cheapest_fractional_path
  - create_plan
  - build_subplan
  - convert_EXISTS_to_ANY
  - subpath_is_hashable
  - makeNode
  - list_make2
- Called from (representative examples):
  - process_sublinks_mutator

## Notes and Other Information
- The function implements sophisticated optimization logic for EXISTS sublinks, potentially creating both a regular SubPlan and a hash-based ANY SubPlan alternative
- Tuple fraction hints are carefully chosen based on sublink type: 1.0 for EXISTS (like LIMIT 1), 0.5 for ALL/ANY (expecting early termination), and 0.0 for others
- The function handles the complexity of parameter passing between outer and inner queries
- Located in src/backend/optimizer/plan/subselect.c:162-318
- Returns either a SubPlan node, a Param node (for InitPlans), or an AlternativeSubPlan node containing multiple execution strategies