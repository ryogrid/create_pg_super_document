# check_agg_arguments_walker

## Location
src/backend/parser/parse_agg.c: 717 - 819

## Overview
A tree-walking function that recursively traverses expression nodes to find variables and aggregates, tracking their nesting levels and validating that prohibited constructs are not nested within aggregate arguments.

## Definition
static bool check_agg_arguments_walker(Node *node, check_agg_arguments_context *context)

## Detailed Description
This function implements a recursive tree walker that visits every node in an expression tree to identify variables (Var nodes), aggregates (Aggref nodes), and grouping functions (GroupingFunc nodes) while tracking their nesting levels. It maintains the minimum variable level and aggregate level found during traversal, adjusting for the current sublevel context to ensure proper frame-of-reference calculations.

The walker also enforces several important restrictions: it prohibits set-returning functions and window functions within aggregate arguments at the top level (they are allowed within subqueries inside the aggregate). When encountering subqueries (Query nodes), it properly adjusts the sublevel counter to maintain correct level calculations across query boundaries.

The function uses PostgreSQL's standard tree-walking infrastructure, recursively descending into subexpressions using either expression_tree_walker for regular expressions or query_tree_walker for subqueries, ensuring comprehensive coverage of the entire expression tree.

## Parameters / Member Variables
- `node`: Current node in the expression tree being examined
- `context`: Context structure containing state information including minimum levels found and current sublevel depth

## Dependencies
- Functions called/Symbols referenced:
  - expression_tree_walker
  - query_tree_walker
  - [exprLocation](../e/exprLocation.md)
  - ereport (for error handling)
- Called from (representative examples):
  - [check_agg_arguments](check_agg_arguments.md)
  - check_ungrouped_columns_context
  - (recursively calls itself)

## Notes and Other Information
- Returns false to continue walking, true to stop (standard walker pattern)
- Adjusts level calculations by subtracting sublevels_up to maintain proper frame of reference
- Ignores local variables and aggregates of subqueries (negative adjusted levels)
- Set-returning functions and window functions are only prohibited at the top level of aggregate arguments
- Properly handles Query nodes by incrementing sublevel counter before recursion
- Uses PostgreSQL's standard tree-walking infrastructure for comprehensive traversal