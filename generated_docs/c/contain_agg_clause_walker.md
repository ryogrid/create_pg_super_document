# contain_agg_clause_walker

## Location
src/backend/optimizer/util/clauses.c: 183 - 213

## Overview
A recursive tree walker function that identifies aggregate function nodes (Aggref and GroupingFunc) within an expression tree.

## Definition


## Detailed Description
This static function serves as the core implementation for aggregate function detection within expression trees. It traverses the node tree recursively using the expression_tree_walker framework, looking specifically for Aggref (aggregate function) and GroupingFunc (grouping function) nodes. When such nodes are found, the function performs assertions to ensure they are at the current aggregation level (agglevelsup == 0) and returns true to abort further traversal. The function also asserts that no SubLink nodes are present, enforcing the constraint that subqueries should have been reduced to subplans before this function is called.

## Parameters / Member Variables
- : A Node pointer representing the current node being examined in the expression tree
- : A void pointer for walker context (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - Aggref
  - GroupingFunc
  - SubLink
  - expression_tree_walker
  - [contain_agg_clause_walker](contain_agg_clause_walker.md) (recursive call)
- Called from (representative examples):
  - [contain_agg_clause](contain_agg_clause.md)
  - max_parallel_hazard_context
  - [contain_agg_clause_walker](contain_agg_clause_walker.md) (recursive)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit
- Uses the PostgreSQL expression tree walker framework for efficient traversal
- Performs runtime assertions to validate aggregate level constraints
- Returns true immediately upon finding an aggregate, short-circuiting the traversal
- The function expects that sublinks have already been processed and converted to subplans
- Part of the aggregate-function clause manipulation utilities in the PostgreSQL optimizer