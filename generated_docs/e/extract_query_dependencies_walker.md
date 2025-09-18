# extract_query_dependencies_walker

## Location
src/backend/optimizer/plan/setrefs.c: 3589 - 3653

## Overview
Tree walker function that recursively traverses query nodes to collect relation OIDs, invalidation items, and row security information for dependency tracking.

## Definition
```c
bool extract_query_dependencies_walker(Node *node, PlannerInfo *context)
```

## Detailed Description
This function serves as the core tree-walking engine for dependency extraction from PostgreSQL query trees. It implements a recursive traversal strategy that handles different node types appropriately - Query nodes receive special processing to extract relation dependencies and handle utility statements, while expression nodes are processed for function dependencies and regclass constants.

The function is designed to be used both internally by `extract_query_dependencies` and externally by `expression_planner_with_deps` for simple expressions. When processing Query nodes, it handles both regular queries and utility statements like CALL, extracting relation OIDs from range table entries and detecting row security policies. For expression nodes, it delegates to `fix_expr_common` to handle function dependencies and regclass constants.

The walker uses standard PostgreSQL tree traversal patterns, returning false to continue traversal and collecting dependency information in the passed `PlannerInfo` context structure.

## Parameters / Member Variables
- `node`: The current node in the query tree being analyzed (can be NULL)
- `context`: PlannerInfo structure containing global state for dependency collection, including relationOids and invalItems lists

## Dependencies
- Functions called/Symbols referenced:
  - IsA
  - Assert
  - [PlaceHolderVar](../P/PlaceHolderVar.md)
  - CMD_UTILITY
  - CallStmt
  - [extract_query_dependencies_walker](extract_query_dependencies_walker.md) (recursive)
  - UtilityContainsQuery
  - RTE_RELATION
  - RTE_SUBQUERY  
  - RTE_NAMEDTUPLESTORE
  - OidIsValid
  - lappend_oid
  - query_tree_walker
  - [fix_expr_common](../f/fix_expr_common.md)
  - expression_tree_walker

- Called from (representative examples):
  - [extract_query_dependencies](extract_query_dependencies.md) (src/backend/optimizer/plan/setrefs.c:3573)
  - [expression_planner_with_deps](expression_planner_with_deps.md) (src/backend/optimizer/plan/planner.c:6717)
  - [extract_query_dependencies_walker](extract_query_dependencies_walker.md) (recursive calls)

## Notes and Other Information
- The function is exported specifically to allow `expression_planner_with_deps` to use it for simple expression dependency extraction
- It includes special handling for utility statements, particularly CALL statements which require processing of function expressions and output arguments
- The function properly handles subqueries and named tuple stores in range table entries, extracting relation OIDs where valid
- Row security detection is accumulated in the `context->glob->dependsOnRole` flag
- The walker pattern ensures complete traversal of complex nested query structures
- [PlaceHolderVar](../P/PlaceHolderVar.md) nodes are explicitly asserted against, indicating they should not appear at this stage of processing