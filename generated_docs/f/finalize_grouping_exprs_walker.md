# finalize_grouping_exprs_walker

## Location
[src/backend/parser/parse_agg.c:1502-1656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L1502-L1656)

## Overview
A tree walker function that finalizes GROUPING expressions by validating their arguments and resolving references to grouping columns, ensuring GROUPING functions only reference valid grouping expressions.

## Definition

```c
static bool
finalize_grouping_exprs_walker(Node *node,
							   check_ungrouped_columns_context *context)
```
## Detailed Description
This function is a recursive tree walker that processes expression trees to finalize GROUPING expressions. It performs several key operations:

1. **Aggregate Handling**: When encountering Aggref nodes at the current query level, it recursively processes only the direct arguments while avoiding normal arguments, ORDER BY arguments, and filters to prevent nested GROUPING expressions.

2. **GROUPING Function Processing**: For GroupingFunc nodes at the appropriate query level, it validates that each argument matches a grouping expression from the current query's GROUP BY clause. It resolves variable references to their corresponding ressortgroupref values and stores these references in the GroupingFunc's refs list.

3. **Query Level Management**: The function correctly handles nested subqueries by adjusting the sublevels_up counter and only processing expressions at the appropriate query level.

4. **Validation**: It enforces the rule that GROUPING function arguments must be exact matches to grouping expressions, rejecting functional dependencies or outer references that would normally be acceptable in other contexts.

## Parameters / Member Variables
- : The expression tree node being processed
- : Context structure containing:
  - : Current nesting level for handling subqueries
  - : List of grouping expressions from the GROUP BY clause
  - : Flag indicating presence of join range table entries
  - : Flag for non-variable grouping expressions
  - : Flag tracking if currently processing aggregate direct arguments
  - : Parse state for error reporting
  - : Query structure for join alias flattening

## Dependencies
- Functions called/Symbols referenced:
  - [flatten_join_alias_vars](flatten_join_alias_vars.md)
  - [equal](../e/equal.md)
  - [exprLocation](../e/exprLocation.md)
  - [lappend_int](../l/lappend_int.md)
  - query_tree_walker
  - expression_tree_walker
  - ereport (for error reporting)
- Called from:
  - [finalize_grouping_exprs](finalize_grouping_exprs.md)
  - Self-recursion for tree traversal

## Notes and Other Information
- This function is part of the PostgreSQL parser's aggregate processing pipeline
- It implements strict validation rules for GROUPING expressions that are more restrictive than normal expression validation
- The function uses the standard PostgreSQL tree walker pattern with context passing
- Error messages provide specific location information for debugging invalid GROUPING usage
- The function handles both variable and non-variable grouping expressions through different code paths