# ExecEvalParamExtern

## Location
src/backend/executor/execExprInterp.c: 2532 - 2578

## Overview
ExecEvalParamExtern evaluates PARAM_EXTERN parameters (external parameters provided by the client) by retrieving their values from the parameter list info, with support for dynamic parameter fetching and type validation.

## Definition
```c
void ExecEvalParamExtern(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
This function handles the evaluation of PARAM_EXTERN parameters, which are external parameters provided by the client application (such as values for prepared statement placeholders like $1, $2, etc.). The function first checks if parameter information is available and the parameter ID is within valid bounds. It supports both static parameter arrays and dynamic parameter fetching via hook functions. The function includes comprehensive type checking to ensure parameter types match what was expected during plan preparation.

If a paramFetch hook is provided, it's called to allow dynamic parameter resolution. Otherwise, the function accesses the static parameter array. The function performs safety checks on parameter types and reports appropriate errors for missing or mismatched parameters.

## Parameters / Member Variables
- `state`: ExprState containing the expression evaluation state
- `op`: ExprEvalStep containing the operation details including parameter ID and expected type
- `econtext`: ExprContext providing access to the parameter list info

## Dependencies
- Functions called/Symbols referenced:
  - ParamListInfo
  - ParamExternData
  - likely (performance hint macro)
  - ereport (error reporting)
  - format_type_be (type formatting)
- Called from (representative examples):
  - ExecInterpExpr
  - FunctionReturningBool (via JIT compilation)

## Notes and Other Information
- Part of PostgreSQL's expression evaluation interpreter framework
- Handles external parameters like $1, $2 in prepared statements
- Supports dynamic parameter fetching through hook functions
- Performs type safety validation against the prepared plan
- Uses likely() macros for performance optimization of common paths
- Reports detailed error messages for parameter type mismatches
- Located in src/backend/executor/execExprInterp.c:2532-2578