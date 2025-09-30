# minmax_qp_callback

## Location
[src/backend/optimizer/plan/planagg.c:478-496](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planagg.c#L478-L496)

## Overview
A callback function used by query_planner() to configure pathkeys for MIN/MAX aggregate optimization subqueries.

## Definition

```c
static void
minmax_qp_callback(PlannerInfo *root, void *extra)
```
## Detailed Description
This callback function is invoked during query planning to customize the pathkey configuration for MIN/MAX aggregate subqueries. It sets up the planner state to focus solely on the sorting requirements needed for the optimization:

1. **Clears Irrelevant Pathkeys**: Sets group_pathkeys, window_pathkeys, and distinct_pathkeys to NIL since MIN/MAX optimization subqueries don't use grouping, windowing, or distinct operations
2. **Configures Sort Pathkeys**: Generates sort pathkeys from the subquery's ORDER BY clause using 
3. **Sets Query Pathkeys**: Makes the sort pathkeys the primary query pathkeys, ensuring the planner prioritizes paths that satisfy the required ordering

This specialized configuration helps the query planner generate optimal index scan paths that can directly return the minimum or maximum values without full table scans or explicit sorting operations.

## Parameters / Member Variables
- : PlannerInfo structure for the MIN/MAX subquery being planned
- : Unused extra parameter (standard callback interface)

## Dependencies
- Functions called/Symbols referenced:
  -  - Converts ORDER BY clauses into pathkey specifications
- Called from (representative examples):
  -  (src/backend/optimizer/plan/planagg.c:420)

## Notes and Other Information
- This callback is specifically designed for the simplified subqueries created by 
- The function eliminates pathkey types that are irrelevant to MIN/MAX optimization (grouping, windowing, distinct)
- By making sort_pathkeys the primary query_pathkeys, it signals to the planner that ordered access paths are preferred
- The callback interface follows the standard query_planner callback pattern with an unused extra parameter

## Simplified Source

```c
static void minmax_qp_callback(PlannerInfo *root, void *extra) {
    // Clear pathkeys not needed for MIN/MAX optimization
    root->group_pathkeys = NIL;
    root->window_pathkeys = NIL;
    root->distinct_pathkeys = NIL;

    // Generate sort pathkeys from ORDER BY clause
    root->sort_pathkeys = make_pathkeys_for_sortclauses(root,
                                                       root->parse->sortClause,
                                                       root->parse->targetList);

    // Make sort order the primary query requirement
    root->query_pathkeys = root->sort_pathkeys;
}
```