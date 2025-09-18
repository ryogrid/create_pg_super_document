# set_cte_pathlist

## Location
[src/backend/optimizer/path/allpaths.c:2860-2938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L2860-L2938)

## Overview
Builds the single access path for a non-self-reference CTE RTE (Range Table Entry), handling pathlist generation for Common Table Expression scans in PostgreSQL's query planner.

## Definition
```c
static void set_cte_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
```

## Detailed Description
This function is responsible for creating access paths for non-self-referencing Common Table Expression (CTE) RTEs in PostgreSQL's query optimizer. CTEs are WITH clauses that define temporary named result sets that can be referenced in the main query. This function handles the complex task of locating the previously planned CTE, extracting its path and plan information, and creating an appropriate scan path for accessing the CTE's results.

The function navigates up the planner hierarchy to find the CTE's definition and corresponding plan, converts pathkeys to the outer query's representation, and handles size estimates. Unlike self-referencing CTEs (recursive CTEs), this handles the simpler case where a CTE is referenced but doesn't reference itself.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global information about the query being planned
- `rel`: RelOptInfo structure representing the relation (CTE) for which paths are being generated
- `rte`: RangeTblEntry representing the CTE reference in the query's range table

## Dependencies
- Functions called/Symbols referenced:
  - CommonTableExpr
  - [list_nth_int](../l/list_nth_int.md)
  - [list_nth](../l/list_nth.md)
  - [set_cte_size_estimates](set_cte_size_estimates.md)
  - [convert_subquery_pathkeys](../c/convert_subquery_pathkeys.md)
  - [add_path](../a/add_path.md)
  - [create_ctescan_path](../c/create_ctescan_path.md)
- Called from (representative examples):
  - [set_rel_size](set_rel_size.md)

## Notes and Other Information
- Does not support join-qual-parameterized paths for CTEs, eliminating the need for a separate set_cte_size phase
- CTE scans do not support pushing join clauses into their quals, but can have required parameterization due to LATERAL references in their target lists
- The function walks up the planner hierarchy using ctelevelsup to find the appropriate CTE definition and plan
- Converts pathkeys from the CTE's context to the outer query's representation using convert_subquery_pathkeys
- Uses plan_id to locate the corresponding path and plan from the global subpaths and subplans lists
- Includes extensive error checking to ensure the referenced CTE exists and has been properly planned
- Located in src/backend/optimizer/path/allpaths.c:2860-2938