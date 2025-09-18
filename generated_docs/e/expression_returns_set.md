# expression_returns_set

## Location
[src/backend/nodes/nodeFuncs.c:758-763](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L758-L763)

## Overview
Tests whether an expression returns a set result, useful for detecting set-returning functions or expressions in query planning and execution.

## Definition
```c
bool expression_returns_set(Node *clause)
```

## Detailed Description
This function serves as a convenient wrapper around `expression_returns_set_walker` to determine if an expression or any sub-expression within it returns a set of values rather than a single value. It uses PostgreSQL's expression tree walker mechanism, making it capable of analyzing not just individual expressions but entire target lists. When applied to target lists, it returns true if any item in the list is a set-returning expression. This is crucial for query planning decisions, particularly when determining if special handling is needed for set-returning functions.

## Parameters / Member Variables
- `clause`: The expression node or target list to examine for set-returning behavior. Can be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - [expression_returns_set_walker](expression_returns_set_walker.md) (performs the actual tree walking and analysis)

- Called from (representative examples):
  - [ExecInitProjectSet](../E/ExecInitProjectSet.md) (executor initialization for projection sets)
  - [check_output_expressions](../c/check_output_expressions.md) (optimizer path analysis)
  - [remove_unused_subquery_outputs](../r/remove_unused_subquery_outputs.md) (subquery optimization)
  - [get_eclass_for_sort_expr](../g/get_eclass_for_sort_expr.md) (equivalence class sorting)
  - [relation_can_be_sorted_early](../r/relation_can_be_sorted_early.md) (early sorting optimization)
  - [subquery_planner](../s/subquery_planner.md) (subquery planning)
  - [make_sort_input_target](../m/make_sort_input_target.md) (sort target creation)
  - [is_simple_values](../i/is_simple_values.md) (VALUES clause analysis)
  - [coerce_to_boolean](../c/coerce_to_boolean.md) (type coercion)
  - [make_row_comparison_op](../m/make_row_comparison_op.md) (row comparison operations)
  - [transformJsonBehavior](../t/transformJsonBehavior.md) (JSON expression transformation)

## Notes and Other Information
- This function is built on PostgreSQL's expression tree walker framework, which provides efficient recursive traversal of expression trees
- Set-returning functions require special handling in both query planning and execution phases
- The function is commonly used in the optimizer to determine if additional projection steps are needed
- Returns false for NULL input, treating it as a non-set-returning case
- Critical for proper handling of functions like generate_series(), unnest(), and other set-returning functions in SQL queries