# pull_varnos_of_level

## Location
src/backend/optimizer/util/var.c: 134 - 154

## Overview
Extracts all distinct variable range table numbers (varnos) present in a parse tree, but only considers Vars at a specified sublevel.

## Definition
```c
Relids pull_varnos_of_level(PlannerInfo *root, Node *node, int levelsup)
```

## Detailed Description
The `pull_varnos_of_level` function creates a set of all the distinct varnos present in a parsetree, but with a key difference from `pull_varnos`: it only considers Vars that are at a specific query nesting level, as specified by the `levelsup` parameter. This is particularly useful when analyzing subqueries and determining which variables belong to which query level in a nested query structure.

The function uses the same walker pattern as `pull_varnos` but configures the context to target a specific sublevel, making it essential for subquery processing and lateral reference analysis.

## Parameters / Member Variables
- `root`: PlannerInfo pointer for the current planning context
- `node`: The Node to analyze for variable range table numbers
- `levelsup`: The specific query nesting level to examine (0 for current level, 1 for parent level, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [pull_varnos_context](pull_varnos_context.md) (struct used for walker context)
  - query_or_expression_tree_walker
  - [pull_varnos_walker](pull_varnos_walker.md)
- Called from (representative examples):
  - [convert_ANY_sublink_to_join](../c/convert_ANY_sublink_to_join.md)
  - [is_simple_subquery](../i/is_simple_subquery.md)
  - [jointree_contains_lateral_outer_refs](../j/jointree_contains_lateral_outer_refs.md)
  - [add_nullingrels_if_needed](../a/add_nullingrels_if_needed.md)

## Notes and Other Information
- The levelsup parameter determines which query level's variables are collected
- Essential for proper handling of nested subqueries and lateral references
- Returns a Relids bitmapset containing varnos from the specified level only
- Used primarily in subquery transformation and lateral reference analysis
- Shares the same walker infrastructure with `pull_varnos` but with level-specific filtering