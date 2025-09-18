# IncrementVarSublevelsUp

## Location
src/backend/rewrite/rewriteManip.c: 850 - 872

## Overview
A public function that increments sublevel counters throughout a query or expression tree to adjust variable references when subquery nesting changes.

## Definition
```c
void IncrementVarSublevelsUp(Node *node, int delta_sublevels_up, int min_sublevels_up)
```

## Detailed Description
IncrementVarSublevelsUp is the main entry point for adjusting sublevel references when query transformations change the nesting depth of subqueries. This function is essential during query optimization operations such as subquery pullup, EXISTS-to-join conversion, and lateral reference handling.

The function sets up a context structure and uses the query_or_expression_tree_walker infrastructure to traverse the entire tree, delegating the actual work to IncrementVarSublevelsUp_walker. It can handle both complete Query structures and bare expression trees, automatically detecting the appropriate traversal method.

The function uses a selective increment approach, only modifying sublevel counters that meet or exceed a specified minimum threshold, which allows for precise control over which parts of nested subquery structures are affected by the transformation.

## Parameters / Member Variables
- `node`: The root node of the tree to process (can be Query or expression node)
- `delta_sublevels_up`: The amount to add to qualifying sublevel counters (typically 1 or -1)
- `min_sublevels_up`: The minimum sublevel threshold - only counters >= this value are modified

## Dependencies
- Functions called/Symbols referenced:
  - query_or_expression_tree_walker
  - IncrementVarSublevelsUp_walker
  - IncrementVarSublevelsUp_context (structure)
  - QTW_EXAMINE_RTES_BEFORE (flag for proper range table processing)
- Called from (representative examples):
  - extract_lateral_references (initsplan.c)
  - build_minmax_path (planagg.c)
  - inline_cte_walker (subselect.c)
  - convert_EXISTS_sublink_to_join (subselect.c)
  - pull_up_simple_subquery (prepjointree.c)
  - flatten_join_alias_vars_mutator (var.c)
  - ReplaceVarsFromTargetList_callback (rewriteManip.c)
  - rewriteSearchAndCycle (rewriteSearchCycle.c)

## Notes and Other Information
- This is a public function declared in rewrite/rewriteManip.h
- Uses query_or_expression_tree_walker to handle both Query and expression tree inputs uniformly
- The QTW_EXAMINE_RTES_BEFORE flag ensures range table entries are processed before the query body
- Widely used throughout the optimizer for query transformation operations
- Essential for maintaining correct variable reference semantics when subquery nesting changes
- The min_sublevels_up parameter provides fine-grained control over which sublevel references are affected
- Commonly used with delta_sublevels_up values of 1 (for pullup operations) or -1 (for pushdown operations)
- Critical for operations like subquery pullup, EXISTS-to-ANY conversion, and lateral reference adjustment
- The function preserves the original tree structure while only modifying sublevel reference counters