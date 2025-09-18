# grouping_planner

## Location
[src/backend/optimizer/plan/planner.c:1335-2076](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L1335-L2076)

## Overview
Performs comprehensive planning steps related to grouping, aggregation, window functions, and other high-level query operations on top of the basic scan/join paths produced by query_planner.

## Definition
```c
static void grouping_planner(PlannerInfo *root, double tuple_fraction, SetOperationStmt *setops)
```

## Detailed Description
This function is the core high-level planner that adds all required top-level processing to the scan/join paths produced by query_planner. It handles the planning of complex SQL operations including:

- **Set Operations**: Plans UNION, INTERSECT, EXCEPT operations through plan_set_operations
- **Grouping and Aggregation**: Creates paths for GROUP BY clauses and aggregate functions
- **Window Functions**: Plans window function execution with proper ordering
- **Sorting**: Implements ORDER BY clauses with various optimization strategies
- **DISTINCT Operations**: Plans DISTINCT clause execution
- **Row Locking**: Adds LockRows nodes for FOR UPDATE/SHARE clauses
- **LIMIT/OFFSET**: Implements result limiting with cost estimation
- **DML Operations**: Adds ModifyTable nodes for INSERT/UPDATE/DELETE/MERGE

The function works by creating a series of upper relations (upperrels) that represent different processing stages, with each stage building upon the previous one. It carefully manages PathTargets to ensure that each stage produces the exact columns needed by subsequent stages.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the query planning context and accumulated state
- `tuple_fraction`: Expected fraction of result tuples to be retrieved (0 = all tuples, 0-1 = fraction, ≥1 = absolute count for LIMIT)  
- `setops`: SetOperationStmt for set operation subqueries, or NULL for regular queries

## Dependencies
- Functions called/Symbols referenced:
  - [preprocess_limit](../p/preprocess_limit.md), plan_set_operations, preprocess_grouping_sets
  - [preprocess_targetlist](../p/preprocess_targetlist.md), preprocess_aggrefs, find_window_functions
  - [query_planner](../q/query_planner.md), create_pathtarget, create_grouping_paths
  - [create_window_paths](../c/create_window_paths.md), create_distinct_paths, create_ordered_paths
  - [create_lockrows_path](../c/create_lockrows_path.md), create_limit_path, create_modifytable_path
- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md)

## Notes and Other Information
- Located in src/backend/optimizer/plan/planner.c:1335-2076
- This is a static function that serves as the main orchestrator for high-level query planning
- The function carefully manages parallel safety throughout the planning process
- Creates and populates the final UPPERREL_FINAL relation that contains all viable execution paths
- Handles both regular queries and set operations with different code paths
- Supports foreign data wrapper (FDW) integration through GetForeignUpperPaths callbacks
- Does not call set_cheapest() - leaves this to the caller
- The function manages complex inheritance hierarchies for DML operations on partitioned tables