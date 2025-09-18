# contain_vars_of_level

## Location
[src/backend/optimizer/util/var.c:441-451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L441-L451)

## Overview
Recursively scans a clause to discover whether it contains any Var nodes of the specified query level.

## Definition


## Detailed Description
The  function performs a boolean test to determine if a given parse tree node or expression contains any variable references (Var nodes, CurrentOfExpr nodes, or PlaceHolderVar nodes) at a specific query nesting level. Unlike  which only checks the current level, this function can check any specified nesting level.

The function uses  with a custom walker callback () to traverse the tree. It properly handles subqueries by adjusting the nesting level context as it recurses. This function is more comprehensive than  as it will recurse into sublinks and can be invoked directly on Query nodes.

This function is particularly useful for:
- Analyzing variable dependencies across query levels
- Subquery optimization and transformation decisions
- Checking for correlated subqueries
- Validating query rewrite rules

## Parameters / Member Variables
- : The root node of the parse tree or expression to examine
- : The target query nesting level to search for (0 = current level, 1 = one level up, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - query_or_expression_tree_walker (tree traversal function)
  - [contain_vars_of_level_walker](contain_vars_of_level_walker.md) (callback function for tree walking)
- Called from (representative examples):
  - [convert_EXISTS_sublink_to_join](convert_EXISTS_sublink_to_join.md) (in src/backend/optimizer/plan/subselect.c)
  - [convert_EXISTS_to_ANY](convert_EXISTS_to_ANY.md) (in src/backend/optimizer/plan/subselect.c)
  - [pull_up_simple_values](../p/pull_up_simple_values.md) (in src/backend/optimizer/prep/prepjointree.c)
  - [apply_child_basequals](../a/apply_child_basequals.md) (in src/backend/optimizer/util/inherit.c)
  - [transformInsertStmt](../t/transformInsertStmt.md) (in src/backend/parser/analyze.c)
  - [transformValuesClause](../t/transformValuesClause.md) (in src/backend/parser/analyze.c)
  - [transformRangeFunction](../t/transformRangeFunction.md) (in src/backend/parser/parse_clause.c)
  - [rewriteRuleAction](../r/rewriteRuleAction.md) (in src/backend/rewrite/rewriteHandler.c)

## Notes and Other Information
- Returns true if any variable at the specified level is found, false otherwise
- Can recurse into sublinks, unlike contain_var_clause
- Can be invoked directly on Query nodes
- Properly handles query nesting level tracking
- More comprehensive than contain_var_clause but potentially more expensive
- Used extensively in subquery processing and query transformation
- Part of PostgreSQL's variable analysis utilities in the optimizer