# ExecEvalJsonCoercionFinish

## Location
[src/backend/executor/execExprInterp.c:4636-4688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L4636-L4688)

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
  - [DatumGetBool](../D/DatumGetBool.md)
  - ereport
  - [GetJsonBehaviorValueString](../G/GetJsonBehaviorValueString.md)
  - [BoolGetDatum](../B/BoolGetDatum.md)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md) (main expression interpreter loop)

## Notes and Other Information
- This function implements SQL/JSON standard error handling semantics
- It differentiates between coercion errors in the main expression versus errors in behavior expressions
- The function resets the error context state after handling errors to allow for reuse
- Part of PostgreSQL's JSON expression evaluation infrastructure introduced for SQL/JSON compliance
- Works in conjunction with ExecEvalJsonCoercion() which performs the actual coercion work

## Simplified Source

```c
void ExecEvalJsonCoercionFinish(ExprState *state, ExprEvalStep *op)
{
    JsonExprState *jsestate = op->d.jsonexpr.jsestate;

    // Check if a soft error occurred during coercion
    if (SOFT_ERROR_OCCURRED(&jsestate->escontext))
    {
        // If error occurred while coercing behavior values, throw proper error
        if (DatumGetBool(jsestate->error.value))
        {
            ereport(ERROR,
                   (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("could not coerce %s expression (%s) to the RETURNING type",
                           "ON ERROR",
                           GetJsonBehaviorValueString(jsestate->jsexpr->on_error)),
                    errdetail("%s", jsestate->escontext.error_data->message)));
        }
        else if (DatumGetBool(jsestate->empty.value))
        {
            ereport(ERROR,
                   (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("could not coerce %s expression (%s) to the RETURNING type",
                           "ON EMPTY",
                           GetJsonBehaviorValueString(jsestate->jsexpr->on_empty)),
                    errdetail("%s", jsestate->escontext.error_data->message)));
        }

        // Set result to NULL and mark error for ON ERROR/ON EMPTY handling
        *op->resvalue = (Datum) 0;
        *op->resnull = true;
        jsestate->error.value = BoolGetDatum(true);

        // Reset error context for next use
        jsestate->escontext.error_occurred = false;
        jsestate->escontext.details_wanted = true;
    }
}
```