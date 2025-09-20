# get_call_expr_arg_stable

## Location
[src/backend/utils/fmgr/fmgr.c:1994-2043](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1994-L2043)

## Overview
Determines whether a specific function argument is constant for the duration of a query by analyzing the calling expression tree directly, identifying stable values for optimization purposes.

## Definition

```c
bool
get_call_expr_arg_stable(Node *expr, int argnum)
```
## Detailed Description
This function examines a calling expression tree to determine if a specific argument will have a constant value throughout query execution. It supports the same expression node types as get_call_expr_argtype and is crucial for query optimization, allowing the executor to identify arguments that can be pre-computed, cached, or used for other performance optimizations.

The function considers two types of expressions as stable: true constants (Const nodes) and external parameters (PARAM_EXTERN), which are bound once per query execution. Future extensions might include other stable expressions like now() function calls.

## Parameters / Member Variables
- : The expression node representing a function call or operator expression to analyze
- : Zero-based index of the argument to check for stability

## Dependencies
- Functions called/Symbols referenced:
  - [list_nth](../l/list_nth.md)
  - list_length
  - IsA (macro)
- Expression node types referenced:
  - FuncExpr
  - OpExpr
  - DistinctExpr
  - ScalarArrayOpExpr
  - NullIfExpr
  - WindowFunc
  - Const
  - Param
- Constants referenced:
  - PARAM_EXTERN
- Called from (representative examples):
  - [get_fn_expr_arg_stable](get_fn_expr_arg_stable.md)

## Notes and Other Information
- Returns false if the expression is NULL, unsupported type, or argnum is out of bounds
- Currently considers only Const nodes and external parameters as stable, but the design allows for future extensions
- External parameters (PARAM_EXTERN) are stable because they are bound once per query and don't change during execution
- This function is essential for optimizations like constant folding and result caching in function execution
- Works at the expression tree level, making it useful during query planning phases before FmgrInfo structures are fully populated