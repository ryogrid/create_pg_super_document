# ExecEvalParamExec

## Location
[src/backend/executor/execExprInterp.c:2510-2531](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2510-L2531)

## Overview
ExecEvalParamExec evaluates PARAM_EXEC parameters (internal executor parameters) by retrieving their values from the executor context's parameter array, implementing lazy evaluation for subplan parameters.

## Definition
```c
void ExecEvalParamExec(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
This function handles the evaluation of PARAM_EXEC parameters, which are internal executor parameters used to pass values between different parts of a query execution plan. The function operates as part of PostgreSQL's expression evaluation framework and implements lazy evaluation - if a parameter hasn't been computed yet (indicated by a non-NULL execPlan), it triggers the execution of the associated subplan to compute the parameter value.

The function accesses parameters through an array index stored in the ExprEvalStep operation, retrieves the ParamExecData from the execution context, and either returns the already-computed value or triggers subplan execution via ExecSetParamPlan.

## Parameters / Member Variables
- `state`: ExprState containing the expression evaluation state
- `op`: ExprEvalStep containing the operation details including the parameter ID
- `econtext`: ExprContext providing access to the parameter execution values array

## Dependencies
- Functions called/Symbols referenced:
  - [ExecSetParamPlan](ExecSetParamPlan.md)
  - [ParamExecData](../P/ParamExecData.md)
  - [ExprEvalStep](ExprEvalStep.md)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (via JIT compilation)

## Notes and Other Information
- Part of PostgreSQL's expression evaluation interpreter framework
- Implements lazy evaluation pattern for subplan parameters
- Uses Assert to verify that ExecSetParamPlan properly processes the parameter
- The unlikely() macro suggests that most parameters are pre-computed
- Located in src/backend/executor/execExprInterp.c:2510-2531

## Simplified Source

```c
void ExecEvalParamExec(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
    // Get parameter data from the execution context array
    ParamExecData *param = &(econtext->ecxt_param_exec_vals[op->d.param.paramid]);

    // If parameter not yet evaluated, execute the subplan to compute it
    if (param->execPlan != NULL) {
        ExecSetParamPlan(param->execPlan, econtext);
    }

    // Return the parameter value and null indicator
    *op->resvalue = param->value;
    *op->resnull = param->isnull;
}
```