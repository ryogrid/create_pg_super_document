# ExecJustConst

## Location
[src/backend/executor/execExprInterp.c:2273-2282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2273-L2282)

## Overview
ExecJustConst is a highly optimized function that evaluates simple constant expressions in PostgreSQL's expression evaluation system.

## Definition
```c
static Datum ExecJustConst(ExprState *state, ExprContext *econtext, bool *isnull)
```

## Detailed Description
This function represents the simplest possible expression evaluation case - returning a constant value. It is part of PostgreSQL's expression compilation optimization where expressions consisting of only a single constant are handled by this specialized function rather than going through the general expression interpreter loop.

The function directly accesses the pre-compiled constant value and null status from the first (and only) evaluation step, making constant expression evaluation extremely efficient with minimal overhead.

## Parameters / Member Variables
- `state`: ExprState structure containing the expression evaluation steps, specifically the constant value data
- `econtext`: ExprContext providing the evaluation context (unused in this simple case)
- `isnull`: Pointer to boolean flag that will be set to indicate if the constant is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [ExprEvalStep](ExprEvalStep.md) (expression evaluation step structure)
  - pg_attribute_always_inline (compiler hint for inlining)
- Called from (representative examples):
  - EEO_JUMP (expression evaluation dispatch mechanism)
  - [ExecReadyInterpretedExpr](ExecReadyInterpretedExpr.md) (expression preparation function)

## Notes and Other Information
- This is a static function within execExprInterp.c, part of the internal expression evaluation machinery
- Represents the most optimized path for constant expressions, avoiding all interpreter overhead
- The function is marked with pg_attribute_always_inline for maximum performance
- Part of PostgreSQL's "just-in-time" expression evaluation optimization system
- Demonstrates PostgreSQL's approach of having specialized handlers for common simple expression patterns