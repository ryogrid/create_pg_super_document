# ExecSetParamPlanMulti

## Location
[src/backend/executor/nodeSubplan.c:1268-1290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSubplan.c#L1268-L1290)

## Overview
ExecSetParamPlanMulti is a function that evaluates multiple not-yet-evaluated initplan output parameters in a batch operation, applying ExecSetParamPlan to each parameter whose ID is listed in the provided bitmapset.

## Definition
```c
void ExecSetParamPlanMulti(const Bitmapset *params, ExprContext *econtext)
```

## Detailed Description
ExecSetParamPlanMulti provides a batch processing mechanism for evaluating initplan parameters. It iterates through a bitmapset of parameter IDs and for each parameter that has not yet been evaluated (indicated by a non-NULL execPlan), it calls ExecSetParamPlan to execute the associated subplan and set the parameter value. This function implements lazy evaluation by only processing parameters that are actually needed and haven't been computed yet.

The function checks each parameter in the provided bitmapset and evaluates only those that are initplan outputs and haven't been processed yet. Parameters that are not initplan outputs are safely ignored. After ExecSetParamPlan processes a parameter, it sets the execPlan field to NULL to indicate the parameter has been evaluated.

## Parameters / Member Variables
- `params`: A bitmapset containing the ParamIDs of parameters to potentially evaluate
- `econtext`: An ExprContext that provides access to the parameter execution values array and can be used for expression evaluation

## Dependencies
- Functions called/Symbols referenced:
  - [bms_next_member](../b/bms_next_member.md)
  - [ParamExecData](../P/ParamExecData.md)
  - [ExecSetParamPlan](ExecSetParamPlan.md)
- Called from (representative examples):
  - [EvalPlanQualBegin](EvalPlanQualBegin.md)
  - [EvalPlanQualStart](EvalPlanQualStart.md)
  - [ExecInitParallelPlan](ExecInitParallelPlan.md)
  - [ExecParallelReinitialize](ExecParallelReinitialize.md)

## Notes and Other Information
- The function uses lazy evaluation - parameters are only computed when needed and haven't been computed before
- Any ExprContext belonging to the current EState can be used, but shorter-lived contexts are preferred for efficiency
- Parameters that are not initplan outputs are ignored without error
- The function ensures that after ExecSetParamPlan processes a parameter, the execPlan field is set to NULL
- This is particularly useful in parallel query execution scenarios where multiple parameters need to be evaluated efficiently