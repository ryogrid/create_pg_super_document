# subquery_planner

## Location
src/backend/optimizer/plan/planner.c: 629 - 1155

## Overview
The subquery_planner function is PostgreSQL's primary per-Query planning routine that performs comprehensive query preprocessing, optimization setup, and delegates to grouping_planner for the main planning work.

## Definition
```c
PlannerInfo *subquery_planner(PlannerGlobal *glob, Query *parse, PlannerInfo *parent_root,
                             bool hasRecursion, double tuple_fraction,
                             SetOperationStmt *setops)
```

## Detailed Description
The subquery_planner function serves as the comprehensive query-level planning coordinator, handling all preprocessing tasks that should be performed exactly once per Query object. It creates and initializes a PlannerInfo structure to track planning state, then systematically processes various query components through multiple optimization phases.

Key processing phases include:
1. PlannerInfo initialization with query-level state management
2. WITH clause processing (CTE handling)
3. MERGE command transformation
4. FROM clause normalization and empty jointree replacement
5. SubLink transformation (EXISTS, ANY subqueries to joins)
6. Function RTE preprocessing and inlining
7. Subquery pullup optimization
8. UNION ALL flattening for simple cases
9. Range table entry classification and analysis
10. Permission checking for view access
11. RowMark preprocessing
12. Comprehensive expression preprocessing across all query components
13. HAVING clause optimization (potential movement to WHERE)
14. Outer join reduction to inner joins where possible
15. Useless RTE_RESULT removal and join tree simplification

After preprocessing, it delegates the core planning work to grouping_planner, then handles final cleanup including parameter identification, initPlan cost accounting, and cheapest path selection.

## Parameters / Member Variables
- `glob`: PlannerGlobal structure containing global planning state shared across all query levels
- `parse`: Query structure produced by parser and rewriter containing the SQL statement to plan
- `parent_root`: PlannerInfo of the immediate parent query (NULL for top-level queries)
- `hasRecursion`: Boolean flag indicating if this is a recursive WITH query requiring special parameter handling
- `tuple_fraction`: Expected fraction of result tuples to be retrieved (affects optimization decisions)
- `setops`: SetOperationStmt context for set operation subqueries to guide path generation (NULL for non-set operations)

## Dependencies
- Functions called/Symbols referenced:
  - SS_process_ctes (WITH clause processing)
  - transform_MERGE_to_join (MERGE transformation)
  - pull_up_sublinks (SubLink optimization)
  - pull_up_subqueries (subquery pullup)
  - preprocess_expression (expression preprocessing)
  - preprocess_qual_conditions (WHERE/JOIN condition processing)
  - grouping_planner (main planning logic)
  - reduce_outer_joins (outer join optimization)
  - SS_identify_outer_params (parameter identification)
  - has_subclass (inheritance checking)
- Called from (representative examples):
  - standard_planner (top-level planning)
  - set_subquery_pathlist (subquery planning)
  - make_subplan (subplan creation)
  - SS_process_ctes (CTE processing)
  - recurse_set_operations (set operation handling)

## Notes and Other Information
- Returns PlannerInfo containing all planning results, with final paths in UPPERREL_FINAL upperrel
- Performs extensive query tree analysis to optimize subsequent planning phases
- Handles complex permission checking for views to prevent information leakage
- Implements sophisticated HAVING clause optimization with group-aware movement to WHERE
- Manages join alias variable cleanup after preprocessing to prevent scan hazards
- Supports both regular and recursive query planning with appropriate parameter handling
- Coordinates with global planner state for cross-query-level optimizations
- Located in src/backend/optimizer/plan/planner.c:629-1155