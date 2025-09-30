# ExecEvalCoerceViaIOSafe

## Location
[src/backend/executor/execExprInterp.c:2579-2638](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2579-L2638)

## Overview
ExecEvalCoerceViaIOSafe performs type coercion through input/output functions in soft-error mode, converting values by serializing to text and deserializing to the target type while gracefully handling conversion errors.

## Definition
```c
void ExecEvalCoerceViaIOSafe(ExprState *state, ExprEvalStep *op)
```

## Detailed Description
This function implements safe type coercion by using the "via IO" method, which converts values between types by first calling the source type's output function to convert the value to its textual representation, then calling the target type's input function to parse that text into the target type. This approach works for any types that have proper input/output functions defined.

The "safe" aspect refers to the use of ErrorSaveContext to catch conversion errors without throwing exceptions. If a conversion error occurs during the input function call, the function sets the result to NULL and returns gracefully instead of propagating the error. This is essential for operations where type conversion failures should be handled as data conditions rather than fatal errors.

The function handles NULL values appropriately by skipping the output function call for NULLs (since output functions aren't called on NULL values) and ensuring the input function receives the correct NULL indicator.

## Parameters / Member Variables
- `state`: ExprState containing the expression evaluation state
- `op`: ExprEvalStep containing the coercion operation details including source value, output/input function info, and result storage

## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCallInfo](../F/FunctionCallInfo.md)
  - [DatumGetCString](../D/DatumGetCString.md)
  - FunctionCallInvoke
  - [ErrorSaveContext](ErrorSaveContext.md)
  - SOFT_ERROR_OCCURRED
  - [ExprEvalStep](ExprEvalStep.md)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (via JIT compilation)

## Notes and Other Information
- Implements EEOP_IOCOERCE_SAFE operation type
- Provides safe type coercion with error handling via ErrorSaveContext
- Uses two-phase conversion: source type → text → target type
- Gracefully handles conversion errors by returning NULL instead of throwing
- Maintains proper NULL handling throughout the conversion process
- Part of PostgreSQL's expression evaluation interpreter framework
- Related to EEOP_IOCOERCE but with added error safety
- Located in src/backend/executor/execExprInterp.c:2579-2638

## Simplified Source

```c
void ExecEvalCoerceViaIOSafe(ExprState *state, ExprEvalStep *op)
{
    char *str;

    // Step 1: Convert source value to string via output function
    if (*op->resnull) {
        str = NULL;  // Output functions not called on nulls
    } else {
        FunctionCallInfo fcinfo_out = op->d.iocoerce.fcinfo_data_out;
        fcinfo_out->args[0].value = *op->resvalue;
        fcinfo_out->args[0].isnull = false;

        str = DatumGetCString(FunctionCallInvoke(fcinfo_out));
    }

    // Step 2: Convert string to target type via input function
    if (!op->d.iocoerce.finfo_in->fn_strict || str != NULL) {
        FunctionCallInfo fcinfo_in = op->d.iocoerce.fcinfo_data_in;
        fcinfo_in->args[0].value = PointerGetDatum(str);
        fcinfo_in->args[0].isnull = *op->resnull;

        // Safe conversion with error handling
        *op->resvalue = FunctionCallInvoke(fcinfo_in);

        if (SOFT_ERROR_OCCURRED(fcinfo_in->context)) {
            // Conversion failed - return NULL instead of error
            *op->resnull = true;
            *op->resvalue = (Datum) 0;
            return;
        }
    }
}
```