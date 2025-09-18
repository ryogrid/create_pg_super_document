# ExecEvalRowNull

## Location
[src/backend/executor/execExprInterp.c:2743-2751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2743-L2751)

## Overview
ExecEvalRowNull evaluates a NULL test (IS NULL) for row expressions, serving as a wrapper that delegates to the internal row null testing implementation.

## Definition
```c
void ExecEvalRowNull(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
This function implements the evaluation of "IS NULL" tests for row expressions in PostgreSQL. It is a simple wrapper function that calls ExecEvalRowNullInt with the "isnull" parameter set to true, indicating that it should test for NULL values rather than NOT NULL. Row NULL testing is used when checking if an entire row constructor or composite value is NULL, which occurs when all fields of the row are NULL.

## Parameters / Member Variables
- `state`: ExprState pointer containing the expression evaluation state
- `op`: ExprEvalStep pointer containing the operation details for the null test
- `econtext`: ExprContext pointer providing the expression evaluation context

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEvalRowNullInt](ExecEvalRowNullInt.md): Internal implementation that performs the actual row null testing
  - ExprEvalStep: Structure containing evaluation step details
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md): Main expression interpreter loop
  - [FunctionReturningBool](../F/FunctionReturningBool.md): JIT compilation type definitions

## Notes and Other Information
- This function is part of the step-based expression evaluation framework
- It specifically handles the "IS NULL" case, while ExecEvalRowNotNull handles "IS NOT NULL"
- The actual logic is implemented in ExecEvalRowNullInt to avoid code duplication
- Row NULL testing follows SQL standard semantics where a row is NULL if all its components are NULL