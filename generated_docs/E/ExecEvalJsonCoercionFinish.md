# ExecEvalJsonCoercionFinish

## Location
src/backend/executor/execExprInterp.c: 4636 - 4688

## Overview
ExecEvalJsonCoercionFinish handles error checking and cleanup after JSON coercion operations in PostgreSQL's expression evaluator, managing ON ERROR and ON EMPTY behavior handling for SQL/JSON expressions.

## Definition
void ExecEvalJsonCoercionFinish(ExprState *state, ExprEvalStep *op)

## Detailed Description
This function serves as a post-processing step for JSON coercion operations in the expression evaluator. It checks if any soft errors occurred during the preceding ExecEvalJsonCoercion() execution and handles them according to SQL/JSON standard error handling semantics. The function distinguishes between errors that should trigger ON ERROR/ON EMPTY handling versus errors that occurred when coercing the JsonBehavior values themselves (which should be thrown as actual errors).

When a soft error is detected, the function examines the JsonExprState to determine the specific context of the error. If the error occurred while coercing an ON ERROR or ON EMPTY behavior expression, it throws a proper error with detailed messages. Otherwise, it sets the result to NULL, marks the error flag, and resets the error context for potential reuse.

## Parameters / Member Variables
- : The ExprState containing the overall expression evaluation context
- : The ExprEvalStep operation descriptor containing the jsonexpr operation data and result storage pointers

## Dependencies
- Functions called/Symbols referenced:
  - SOFT_ERROR_OCCURRED
  - DatumGetBool
  - ereport
  - GetJsonBehaviorValueString
  - BoolGetDatum
- Called from (representative examples):
  - ExecInterpExpr (main expression interpreter loop)

## Notes and Other Information
- This function implements SQL/JSON standard error handling semantics
- It differentiates between coercion errors in the main expression versus errors in behavior expressions
- The function resets the error context state after handling errors to allow for reuse
- Part of PostgreSQL's JSON expression evaluation infrastructure introduced for SQL/JSON compliance
- Works in conjunction with ExecEvalJsonCoercion() which performs the actual coercion work