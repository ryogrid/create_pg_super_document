# get_fn_expr_variadic

## Location
src/backend/utils/fmgr/fmgr.c: 2044 - 2069

## Overview
Retrieves the VARIADIC flag from a function invocation to determine if the function was called with variadic argument syntax, primarily useful for VARIADIC ANY functions.

## Definition


## Detailed Description
This function examines the function expression within an FmgrInfo structure to determine whether the function was invoked using variadic syntax (with the VARIADIC keyword). This information is particularly important for functions declared as VARIADIC ANY, which need to distinguish between cases where they were called with individual arguments versus an array argument that should be expanded.

The function only works with FuncExpr nodes, as other expression types (operators, etc.) do not support variadic syntax. The funcvariadic flag indicates whether the parser recognized a VARIADIC call syntax during query parsing.

## Parameters / Member Variables
- : Pointer to FmgrInfo structure containing function metadata and expression tree

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro)
- Expression node types referenced:
  - FuncExpr
- Called from (representative examples):
  - [count_nulls](../c/count_nulls.md)
  - [concat_internal](../c/concat_internal.md)
  - [text_format](../t/text_format.md)
  - [extract_variadic_args](../e/extract_variadic_args.md)

## Notes and Other Information
- Returns false if FmgrInfo is NULL, fn_expr is not initialized, or the expression is not a FuncExpr
- The default assumption is false when information is not available
- Specifically designed for VARIADIC ANY functions that need to handle arguments differently based on call syntax
- The funcvariadic flag is set during query parsing when the VARIADIC keyword is explicitly used in function calls
- Essential for proper argument handling in variadic functions like concat(), format(), and statistical functions that accept variable numbers of arguments