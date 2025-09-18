# evaluate_function

## Location
src/backend/optimizer/util/clauses.c: 4425 - 4550

## Overview
Attempts to pre-evaluate a function call during query optimization by checking if the function can be simplified to a constant value based on its inputs and volatility properties.

## Definition


## Detailed Description
This function performs constant folding optimization on function calls. It can simplify function calls in two main scenarios:
1. For strict functions with any constant-NULL inputs: returns a NULL constant since the function will never be called
2. For immutable functions (or stable functions in estimation mode) with all constant inputs: actually evaluates the function and returns the result as a Const node

The function includes several safety checks to prevent simplification when inappropriate, such as functions that return sets, functions that return RECORD type, or functions with non-constant inputs. It respects PostgreSQL's function volatility categories and only evaluates immutable functions normally, though it allows stable function evaluation during estimation phases.

## Parameters / Member Variables
- : OID of the function to evaluate
- : Expected result type OID of the function
- : Type modifier for the result
- : Collation ID for the result
- : Collation ID for the inputs
- : List of function arguments
- : Whether the function is variadic
- : HeapTuple containing the function's catalog entry
- : Evaluation context containing optimization settings

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc (function catalog entry structure)
  - [makeNullConst](../m/makeNullConst.md) (creates NULL constant nodes)
  - PROVOLATILE_IMMUTABLE, PROVOLATILE_STABLE (volatility constants)
  - FuncExpr (function expression node type)
  - COERCE_EXPLICIT_CALL (coercion type constant)
  - [evaluate_expr](evaluate_expr.md) (actually evaluates the expression)
- Called from:
  - [simplify_function](../s/simplify_function.md) (main function simplification routine)

## Notes and Other Information
- Returns NULL if the function cannot be simplified, otherwise returns a simplified Expr
- Cannot simplify functions that return sets (proretset = true)
- Cannot simplify functions that return RECORD type due to tuple descriptor complexity
- For strict functions, any NULL input results in immediate NULL output optimization
- In estimation mode, stable functions can be evaluated in addition to immutable ones
- The function builds a temporary FuncExpr node before calling evaluate_expr to perform the actual evaluation
- Located in src/backend/optimizer/util/clauses.c at lines 4425-4550