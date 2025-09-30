# get_windowfunc_expr

## Location
[src/backend/utils/adt/ruleutils.c:10715-10725](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L10715-L10725)

## Overview
A wrapper function that parses back a WindowFunc node into its string representation by delegating to the more comprehensive get_windowfunc_expr_helper function.

## Definition

```c
static void
get_windowfunc_expr(WindowFunc *wfunc, deparse_context *context)
```
## Detailed Description
This function serves as a simplified interface to the WindowFunc deparsing functionality. It takes a WindowFunc node and a deparse context, then immediately calls get_windowfunc_expr_helper with default parameters (NULL for both window name and frame options, and false for the show window name flag). This design pattern provides a clean, minimal interface for the most common case of deparsing window functions while allowing the helper function to handle more complex scenarios with additional parameters.

## Parameters / Member Variables
- : Pointer to the WindowFunc node to be deparsed into string representation
- : Pointer to the deparse_context containing state and configuration for the deparsing operation

## Dependencies
- Functions called/Symbols referenced:
  - [get_windowfunc_expr_helper](get_windowfunc_expr_helper.md)
  - [WindowFunc](../W/WindowFunc.md) (struct type)
  - [deparse_context](../d/deparse_context.md) (struct type)
- Called from (representative examples):
  - [get_rule_expr](get_rule_expr.md)

## Notes and Other Information
This function is part of PostgreSQL's rule deparsing system, which converts internal query tree structures back into SQL text. The WindowFunc node represents window function calls in the query tree, and this function is responsible for converting them back to their SQL syntax. The actual work is delegated to get_windowfunc_expr_helper, making this function a convenience wrapper for the most common deparsing scenario.

## Simplified Source

```c
static void get_windowfunc_expr(WindowFunc *wfunc, deparse_context *context)
{
    // Delegate to helper function with default parameters
    get_windowfunc_expr_helper(wfunc, context, NULL, NULL, false);
}
```