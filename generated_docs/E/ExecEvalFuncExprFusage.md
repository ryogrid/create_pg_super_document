# ExecEvalFuncExprFusage

## Location
src/backend/executor/execExprInterp.c: 2452 - 2472

## Overview
Out-of-line helper function that executes a function call expression (EEOP_FUNCEXPR_FUSAGE) with function usage statistics tracking enabled.

## Definition
```c
void ExecEvalFuncExprFusage(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
This function handles the evaluation of function call expressions where PostgreSQL's function usage statistics (pg_stat_user_functions) tracking is enabled. It wraps the actual function call with pgstat_init_function_usage() and pgstat_end_function_usage() calls to properly track function execution statistics. The function retrieves the FunctionCallInfo from the expression step, calls the target function through the function pointer, and stores the result in the designated output locations while properly handling NULL values.

## Parameters / Member Variables
- `state`: Pointer to the ExprState containing expression execution context
- `op`: Pointer to the ExprEvalStep containing function call information and result storage locations
- `econtext`: Pointer to the ExprContext providing evaluation context (parameter unused in function body)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_init_function_usage](../p/pgstat_init_function_usage.md) (initialize function usage tracking)
  - [pgstat_end_function_usage](../p/pgstat_end_function_usage.md) (finalize function usage tracking)
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (structure type for function call parameters)
  - PgStat_FunctionCallUsage (structure type for usage statistics)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md) (main expression interpreter at line 775)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (in LLVM JIT compilation)

## Notes and Other Information
- Used specifically for EEOP_FUNCEXPR_FUSAGE opcode execution
- Part of PostgreSQL's statistics collection system for user-defined functions
- Stores function result in op->resvalue and NULL status in op->resnull
- The econtext parameter is provided for consistency but not used in this implementation
- Essential for performance monitoring and analysis of user-defined functions