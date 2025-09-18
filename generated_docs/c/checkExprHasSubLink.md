# checkExprHasSubLink

## Location
[src/backend/rewrite/rewriteManip.c:296-308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L296-L308)

## Overview
Checks if an expression contains a SubLink node, which represents subqueries within expressions.

## Definition
```c
bool checkExprHasSubLink(Node *node)
```

## Detailed Description
This function traverses an expression tree or Query node to determine whether it contains any SubLink nodes. SubLinks represent subqueries that appear within expressions, such as EXISTS clauses, IN/ANY/ALL subqueries, and scalar subqueries. The function uses the query_or_expression_tree_walker infrastructure with the QTW_IGNORE_RC_SUBQUERIES flag, which means it will examine the main query structure but will not recurse into subqueries that are in the range table or CTE list. This selective traversal ensures that the function only detects SubLinks that are embedded within expressions of the current query level, not those that are part of the query's structure itself.

## Parameters / Member Variables
- `node`: The root node (Query or expression tree) to examine for SubLink nodes

## Dependencies
- Functions called/Symbols referenced:
  - query_or_expression_tree_walker (tree traversal function)
  - [checkExprHasSubLink_walker](checkExprHasSubLink_walker.md) (helper walker function)
  - [QTW_IGNORE_RC_SUBQUERIES](../Q/QTW_IGNORE_RC_SUBQUERIES.md) (flag to control traversal behavior)
- Called from (representative examples):
  - [RelationBuildRowSecurity](../R/RelationBuildRowSecurity.md)
  - [flatten_join_alias_vars_mutator](../f/flatten_join_alias_vars_mutator.md)
  - [rewriteRuleAction](../r/rewriteRuleAction.md)
  - [rewriteTargetView](../r/rewriteTargetView.md)
  - [AddQual](../A/AddQual.md)
  - [replace_rte_variables_mutator](../r/replace_rte_variables_mutator.md)

## Notes and Other Information
- Returns true if any SubLink is found, false otherwise
- Uses QTW_IGNORE_RC_SUBQUERIES flag to avoid examining subqueries in range tables or CTEs
- Can handle both Query nodes and bare expression trees as input
- Primarily used in query rewriting and optimization contexts to detect the presence of subqueries
- Part of the rewrite manipulation infrastructure for analyzing query structure
- The selective traversal behavior is crucial for distinguishing between SubLinks that are part of expressions versus those that are structural elements of the query