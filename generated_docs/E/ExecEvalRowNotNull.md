# ExecEvalRowNotNull

## Location
[src/backend/executor/execExprInterp.c:2752-2758](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2752-L2758)

## Overview
ExecEvalRowNotNull evaluates a NOT NULL test (IS NOT NULL) for row expressions, serving as a wrapper that delegates to the internal row null testing implementation.

## Definition
```c
void ExecEvalRowNotNull(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
This function implements the evaluation of "IS NOT NULL" tests for row expressions in PostgreSQL. It is a simple wrapper function that calls ExecEvalRowNullInt with the "isnull" parameter set to false, indicating that it should test for NOT NULL values rather than NULL. Row NOT NULL testing is used when checking if an entire row constructor or composite value is NOT NULL, which occurs when at least one field of the row is NOT NULL.

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
- It specifically handles the "IS NOT NULL" case, while ExecEvalRowNull handles "IS NULL"
- The actual logic is implemented in ExecEvalRowNullInt to avoid code duplication
- Row NOT NULL testing follows SQL standard semantics where a row is NOT NULL if any of its components are NOT NULL