# generate_series_step_numeric

## Location
[src/backend/utils/adt/numeric.c:1707-1844](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1707-L1844)

## Overview
Implements a set-returning function that generates a series of numeric values between start and stop values with a configurable step size.

## Definition

```c
Datum
generate_series_step_numeric(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL set-returning function (SRF) that generates a sequence of numeric values. It supports both two-parameter (start, stop) and three-parameter (start, stop, step) variants. When only two parameters are provided, it defaults to a step of 1. The function validates that start, stop, and step values are not NaN or infinity, and that step is not zero.

The function uses PostgreSQL's SRF framework to maintain state across multiple calls, storing the current position, stop value, and step in a context structure. It handles both positive and negative step values, determining the appropriate termination condition based on the step direction.

## Parameters / Member Variables
- Parameter 0: start_num - The starting value of the series
- Parameter 1: stop_num - The ending value of the series  
- Parameter 2 (optional): step_num - The increment/decrement value (defaults to 1)
- Context structure members:
  - : Current position in the series (NumericVar)
  - : End value of the series (NumericVar)
  - : Step increment/decrement (NumericVar)

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL - Check if this is the first call to the SRF
  - PG_GETARG_NUMERIC - Extract numeric arguments
  - NUMERIC_IS_SPECIAL, NUMERIC_IS_NAN - Check for special values
  - PG_NARGS - Get number of function arguments
  - [init_var_from_num](../i/init_var_from_num.md), set_var_from_num, set_var_from_var - [NumericVar](../N/NumericVar.md) operations
  - [cmp_var](../c/cmp_var.md) - Compare numeric variables
  - [add_var](../a/add_var.md) - Add numeric variables
  - [make_result](../m/make_result.md) - Convert NumericVar to Numeric
  - SRF_FIRSTCALL_INIT, SRF_PERCALL_SETUP - SRF framework functions
  - SRF_RETURN_NEXT, SRF_RETURN_DONE - SRF return macros
  - [NumericGetDatum](../N/NumericGetDatum.md) - Convert Numeric to Datum
- Called from:
  - [generate_series_numeric](generate_series_numeric.md) (wrapper for two-parameter version)

## Notes and Other Information
- Located in src/backend/utils/adt/numeric.c:1707-1844
- Implements PostgreSQL's set-returning function protocol for maintaining state across calls
- Validates input parameters to reject NaN, infinity, and zero step values
- Handles both ascending (positive step) and descending (negative step) series
- Uses appropriate memory contexts for multi-call persistence
- Part of PostgreSQL's generate_series function family for numeric data types
- The function context persists current position and parameters between calls

## Simplified Source

```c
Datum
generate_series_step_numeric(PG_FUNCTION_ARGS)
{
    generate_series_numeric_fctx *fctx;
    FuncCallContext *funcctx;
    MemoryContext oldcontext;

    if (SRF_IS_FIRSTCALL())
    {
        Numeric start_num = PG_GETARG_NUMERIC(0);
        Numeric stop_num = PG_GETARG_NUMERIC(1);
        NumericVar steploc = const_one;

        // Validate start and stop values (no NaN/infinity)
        if (NUMERIC_IS_SPECIAL(start_num) || NUMERIC_IS_SPECIAL(stop_num))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("start/stop values cannot be NaN or infinity")));

        // Handle optional step parameter
        if (PG_NARGS() == 3)
        {
            Numeric step_num = PG_GETARG_NUMERIC(2);
            if (NUMERIC_IS_SPECIAL(step_num))
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("step size cannot be NaN or infinity")));

            init_var_from_num(step_num, &steploc);
            if (cmp_var(&steploc, &const_zero) == 0)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("step size cannot equal zero")));
        }

        // Initialize SRF context and state
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        fctx = palloc(sizeof(generate_series_numeric_fctx));
        init_var(&fctx->current);
        init_var(&fctx->stop);
        init_var(&fctx->step);

        set_var_from_num(start_num, &fctx->current);
        set_var_from_num(stop_num, &fctx->stop);
        set_var_from_var(&steploc, &fctx->step);

        funcctx->user_fctx = fctx;
        MemoryContextSwitchTo(oldcontext);
    }

    // Per-call processing
    funcctx = SRF_PERCALL_SETUP();
    fctx = funcctx->user_fctx;

    // Check termination condition based on step direction
    bool continue_series = (fctx->step.sign == NUMERIC_POS &&
                           cmp_var(&fctx->current, &fctx->stop) <= 0) ||
                          (fctx->step.sign == NUMERIC_NEG &&
                           cmp_var(&fctx->current, &fctx->stop) >= 0);

    if (continue_series)
    {
        Numeric result = make_result(&fctx->current);

        // Advance to next value
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        add_var(&fctx->current, &fctx->step, &fctx->current);
        MemoryContextSwitchTo(oldcontext);

        SRF_RETURN_NEXT(funcctx, NumericGetDatum(result));
    }
    else
        SRF_RETURN_DONE(funcctx);
}
```