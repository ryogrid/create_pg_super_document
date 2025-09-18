# ExecInterpExprStillValid

## Location
[src/backend/executor/execExprInterp.c:1915-1934](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L1915-L1934)

## Overview
A validation wrapper function that performs schema compatibility checks on the first execution of an expression, then switches to the actual evaluation function for subsequent executions.

## Definition
```c
Datum ExecInterpExprStillValid(ExprState *state, ExprContext *econtext, bool *isNull)
```

## Detailed Description
This function serves as a one-time validation gate for expression evaluation in PostgreSQL. It implements a lazy validation pattern where schema compatibility checks are deferred until the first actual execution of an expression.

The function operates in two phases:
1. **First execution**: Calls CheckExprStillValid() to verify that the compiled expression is still compatible with the current schema (e.g., checking that Var nodes still reference valid attributes after potential schema changes)
2. **Subsequent executions**: Updates state->evalfunc to point directly to the actual evaluation function stored in state->evalfunc_private, bypassing the validation overhead

This design pattern allows PostgreSQL to handle cases where:
- Schema changes occur between expression compilation and execution
- DDL operations modify table structures that expressions depend on
- Plans are cached and reused across schema modifications

After the first execution, the function pointer is updated to eliminate the validation overhead for performance-critical repeated evaluations.

## Parameters / Member Variables
- `state`: Pointer to ExprState containing the expression and evaluation function pointers
- `econtext`: Expression context providing tuple slots and parameter values  
- `isNull`: Output parameter set to true if the result is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [CheckExprStillValid](../C/CheckExprStillValid.md) (validation function)
  - [state](../s/state.md)->evalfunc (actual evaluation function after validation)
- Called from:
  - [ExecReadyInterpretedExpr](ExecReadyInterpretedExpr.md) (set as initial evalfunc)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (LLVM JIT compilation context)

## Notes and Other Information
- Declared with extern linkage so it can be used by other expression execution methods (including JIT)
- Implements a self-modifying function pointer pattern for performance optimization
- Critical for handling schema evolution in long-running connections and prepared statements
- The validation check is performed only once per expression state, making it efficient for repeated evaluations
- Part of PostgreSQL's expression evaluation infrastructure that bridges compilation-time optimizations with runtime validation