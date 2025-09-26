# FuncExpr

## Location
src/include/nodes/primnodes.h: 746 - 771

## Overview
FuncExpr represents a function call expression node in PostgreSQL's query tree, encapsulating all information needed to execute function calls including arguments, result types, and display formatting.

## Definition


## Detailed Description
FuncExpr is the fundamental expression node for representing function calls in PostgreSQL's SQL execution engine. It stores all necessary metadata about the function being called, including its catalog identifier, return type information, argument list, and execution parameters.

The structure supports various function call forms including regular functions, set-returning functions, variadic functions, and functions with different display formats. Most type and formatting information is marked as query_jumble_ignore to ensure that functionally equivalent queries produce the same plan cache keys regardless of minor representation differences.

## Parameters / Member Variables
- : Base expression node structure (inherited from Expr)
- : OID of the function from the pg_proc system catalog
- : Data type OID of the function's return value
- : Boolean flag indicating if the function returns a set of values rather than a single value
- : Boolean flag indicating if variadic arguments have been combined into an array as the last argument
- : Enumeration specifying how the function call should be displayed (e.g., COERCE_EXPLICIT_CALL, COERCE_IMPLICIT_CAST)
- : Collation OID for the function result
- : Collation OID that the function should use for input processing
- : List of argument expressions passed to the function
- : Parse location in the original query text for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - CoercionForm (for function display formatting)
  - ParseLoc (for location tracking)
  - Expr (base expression structure)
  - List (for argument expressions)
  
- Called from (representative examples):
  - makeFuncExpr (function call creation utility)
  - ParseFuncOrColumn (parser transformation of function calls)
  - ExecInitExprRec (executor initialization for expressions)
  - evaluate_function (optimizer constant folding)
  - simplify_function (optimizer function simplification)
  - get_func_expr (rule output formatting)

## Notes and Other Information
- Central to PostgreSQL's function call mechanism, supporting both built-in and user-defined functions
- The funcretset flag enables special handling for set-returning functions in contexts like SELECT lists and FROM clauses
- Supports variadic functions through the funcvariadic flag and argument array combination
- Multiple fields marked as query_jumble_ignore ensure consistent plan caching across equivalent function calls
- The funcformat field controls how function calls are displayed in query output and error messages
- Used extensively in query optimization for function inlining, constant folding, and cost estimation
- Supports both scalar functions and set-returning functions with different execution semantics
- Essential component of PostgreSQL's extensible function system, enabling custom function integration
- Collation support allows proper handling of locale-sensitive function operations