# set_subquery_pathlist

## Location
src/backend/optimizer/path/allpaths.c: 2482 - 2748

## Overview
Generates SubqueryScan access paths for a subquery RTE by planning the subquery and creating corresponding outer query paths.

## Definition
```c
static void set_subquery_pathlist(PlannerInfo *root, RelOptInfo *rel,
                                  Index rti, RangeTblEntry *rte)
```

## Detailed Description
This function is responsible for creating access paths for subqueries in the PostgreSQL query planner. It performs several key optimizations: (1) attempts to push down WHERE clauses from the outer query into the subquery to improve subquery planning, (2) tries to create window function run conditions for early termination, (3) removes unused output columns from the subquery, (4) plans the subquery using subquery_planner(), and (5) creates SubqueryScan paths in the outer query for each path produced by the subquery planner. The function handles both regular and parallel paths, and includes special logic for LATERAL subqueries and security barrier views. It also determines tuple_fraction hints to pass to the subquery planner based on the outer query's characteristics.

## Parameters / Member Variables
- `root`: PlannerInfo for the current query level
- `rel`: RelOptInfo for the subquery relation being planned
- `rti`: Range table index of the subquery RTE
- `rte`: RangeTblEntry containing the subquery to be planned

## Dependencies
- Functions called/Symbols referenced:
  - copyObject (deep copy the subquery)
  - memset, palloc0 (memory management)
  - [subquery_is_pushdown_safe](subquery_is_pushdown_safe.md) (check if clauses can be pushed down)
  - [qual_is_pushdown_safe](../q/qual_is_pushdown_safe.md) (check individual clause safety)
  - [subquery_push_qual](subquery_push_qual.md) (push clause into subquery)
  - [check_and_push_window_quals](../c/check_and_push_window_quals.md) (attempt window run conditions)
  - [remove_unused_subquery_outputs](../r/remove_unused_subquery_outputs.md) (optimize subquery output)
  - [subquery_planner](subquery_planner.md) (plan the subquery)
  - fetch_upper_rel (get final relation from subquery)
  - [set_dummy_rel_pathlist](set_dummy_rel_pathlist.md) (handle empty subqueries)
  - [set_subquery_size_estimates](set_subquery_size_estimates.md) (set size estimates)
  - [convert_subquery_pathkeys](../c/convert_subquery_pathkeys.md) (convert pathkeys to outer context)
  - [create_subqueryscan_path](../c/create_subqueryscan_path.md) (create SubqueryScan paths)
  - [add_path](../a/add_path.md), add_partial_path (add paths to relation)
- Called from (representative examples):
  - pushdown_safe_type
  - [set_rel_size](set_rel_size.md)

## Notes and Other Information
- This is a static function accessible only within allpaths.c
- Does not currently support parameterized paths by pushing join clauses into subqueries
- Handles LATERAL subqueries by setting required_outer appropriately
- Respects security_barrier flag to prevent leaky function pushdown in views with RLS
- Uses pushdown_safety_info structure to track reasons why columns are unsafe for pushdown
- Determines whether to pass tuple_fraction hint based on outer query complexity
- Creates both regular and parallel SubqueryScan paths when appropriate
- Optimizes for trivial pathtargets (direct column references in order)
- Located in src/backend/optimizer/path/allpaths.c at lines 2482-2748
- Central to subquery optimization and one of the more complex functions in the path generation system