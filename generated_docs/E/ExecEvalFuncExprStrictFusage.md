# ExecEvalFuncExprStrictFusage

## Location
src/backend/executor/execExprInterp.c: 2473 - 2509

## Overview
Out-of-line helper function that executes a strict function call expression (EEOP_FUNCEXPR_STRICT_FUSAGE) with NULL argument checking and function usage statistics tracking.

## Definition
```c
void ExecEvalFuncExprStrictFusage(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
This function handles the evaluation of strict function call expressions where PostgreSQL's function usage statistics tracking is enabled. Strict functions are those that return NULL if any of their arguments are NULL, without actually calling the underlying function. The function first iterates through all arguments to check for NULL values - if any argument is NULL, it immediately sets the result to NULL and returns. Only if all arguments are non-NULL does it proceed to wrap the actual function call with usage statistics tracking via pgstat_init_function_usage() and pgstat_end_function_usage().

## Parameters / Member Variables
- `state`: Pointer to the ExprState containing expression execution context
- `op`: Pointer to the ExprEvalStep containing function call information, argument count, and result storage locations
- `econtext`: Pointer to the ExprContext providing evaluation context (parameter unused in function body)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_init_function_usage](../p/pgstat_init_function_usage.md) (initialize function usage tracking)
  - [pgstat_end_function_usage](../p/pgstat_end_function_usage.md) (finalize function usage tracking)
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (structure type for function call parameters)
  - PgStat_FunctionCallUsage (structure type for usage statistics)
  - [NullableDatum](../N/NullableDatum.md) (structure type for nullable arguments)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md) (main expression interpreter at line 783)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (in LLVM JIT compilation)

## Notes and Other Information
- Used specifically for EEOP_FUNCEXPR_STRICT_FUSAGE opcode execution
- Implements strict function semantics: NULL in, NULL out
- Part of PostgreSQL's statistics collection system for user-defined functions
- Performs early NULL detection to avoid unnecessary function calls
- Stores function result in op->resvalue and NULL status in op->resnull
- More efficient than non-strict version when NULL arguments are present