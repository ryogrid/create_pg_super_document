# ExecJustApplyFuncToCase

## Location
src/backend/executor/execExprInterp.c: 2235 - 2272

## Overview
ExecJustApplyFuncToCase is a specialized function that evaluates a CASE_TESTVAL expression and applies a strict function to the result, optimized for expression interpretation in PostgreSQL.

## Definition
```c
static Datum ExecJustApplyFuncToCase(ExprState *state, ExprContext *econtext, bool *isnull)
```

## Detailed Description
This function is part of PostgreSQL's expression evaluation infrastructure, specifically designed to handle CASE expressions efficiently. It first evaluates the CASE_TESTVAL (the value being tested in a CASE expression) by copying the test value and its null status from the case test data structure. Then it applies a strict function to this value.

The function is optimized for performance by being part of the "just-in-time" expression evaluation system, where simple expression patterns are handled by specialized functions rather than the general interpreter loop. The strict function application means that if any argument is NULL, the function immediately returns NULL without calling the actual function.

## Parameters / Member Variables
- `state`: ExprState structure containing the expression evaluation steps and current state
- `econtext`: ExprContext providing the evaluation context including variable values and memory contexts  
- `isnull`: Pointer to boolean flag that will be set to indicate if the result is NULL

## Dependencies
- Functions called/Symbols referenced:
  - ExprEvalStep (expression evaluation step structure)
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (function call information structure)
  - [NullableDatum](../N/NullableDatum.md) (nullable datum structure for function arguments)
- Called from (representative examples):
  - EEO_JUMP (expression evaluation dispatch mechanism)
  - [ExecReadyInterpretedExpr](ExecReadyInterpretedExpr.md) (expression preparation function)

## Notes and Other Information
- This is a static function within execExprInterp.c, part of the internal expression evaluation machinery
- Contains a TODO comment suggesting potential optimization through redesign of the CaseTestExpr mechanism
- Implements strict function semantics where NULL inputs produce NULL output without function execution
- Part of PostgreSQL's expression compilation/interpretation optimization system