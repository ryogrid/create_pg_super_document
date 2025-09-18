# ExecEvalSysVar

## Location
src/backend/executor/execExprInterp.c: 4997 - 5016

## Overview
ExecEvalSysVar evaluates system variables during expression execution by fetching system attributes from tuple slots in PostgreSQL executor.

## Definition
void ExecEvalSysVar(ExprState *state, ExprEvalStep *op, ExprContext *econtext, TupleTableSlot *slot)

## Detailed Description
This function is part of PostgreSQL expression evaluation infrastructure and specifically handles the evaluation of system variables/attributes. It retrieves system attributes from a tuple slot using the attribute number specified in the evaluation step. The function is designed to be called during expression interpretation and ensures that system attributes are properly fetched and validated. It includes error checking to ensure that the attribute fetch operation succeeds, throwing an error if a null value is unexpectedly returned.

## Parameters / Member Variables
- state: ExprState pointer containing the current expression evaluation state
- op: ExprEvalStep pointer containing the evaluation operation details, including the attribute number to fetch
- econtext: ExprContext pointer providing the execution context for expression evaluation
- slot: TupleTableSlot pointer from which to fetch the system attribute

## Dependencies
- Functions called/Symbols referenced:
  - slot_getsysattr
  - elog
- Called from (representative examples):
  - ExecInterpExpr
  - FunctionReturningBool (in JIT compilation context)

## Notes and Other Information
- The function relies on slot_getsysattr having sufficient defenses against invalid attribute numbers
- Includes an unlikely error check to catch cases where system attributes unexpectedly return null values
- Part of the expression evaluation step execution framework used in PostgreSQL tuple processing
- Located in src/backend/executor/execExprInterp.c at lines 4997-5016