# generate_series_step_int8

## Location
[src/backend/utils/adt/int8.c:1383-1458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1383-L1458)

## Overview
Implements the core logic for generating a series of 64-bit integers with configurable start, finish, and step values, using PostgreSQL's set-returning function framework.

## Definition
```c
Datum generate_series_step_int8(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the main implementation of PostgreSQL's generate_series function for int8 (bigint) data types. It generates a sequence of 64-bit integer values starting from a given value, ending at a specified finish value, and incrementing by a configurable step size. The function uses PostgreSQL's Set-Returning Function (SRF) framework to return multiple values across multiple calls.

The function handles both positive and negative step values, performs overflow checking to prevent infinite loops, and validates that the step size is not zero. It maintains state between function calls using a custom context structure to track the current position in the series.

The implementation follows PostgreSQL's standard SRF pattern with initialization on the first call and iteration logic on subsequent calls.

## Parameters / Member Variables
- start (int64): The first value in the series (PG_GETARG_INT64(0))
- finish (int64): The last value in the series (PG_GETARG_INT64(1)) 
- step (int64): The increment between values, defaults to 1 if not provided (PG_GETARG_INT64(2))

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL (macro for first call detection)
  - PG_GETARG_INT64 (macro for extracting int64 arguments)
  - PG_NARGS (macro for argument count)
  - SRF_FIRSTCALL_INIT (SRF initialization)
  - SRF_PERCALL_SETUP (SRF per-call setup)
  - SRF_RETURN_NEXT (return next value in series)
  - SRF_RETURN_DONE (indicate series completion)
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md) (safe addition with overflow detection)
  - [Int64GetDatum](../I/Int64GetDatum.md) (convert int64 to Datum)
  - generate_series_fctx (context structure for state)
  - [FuncCallContext](../F/FuncCallContext.md) (PostgreSQL function call context)

- Called from (representative examples):
  - [generate_series_int8](generate_series_int8.md) (wrapper function)

## Notes and Other Information
- Validates that step size cannot be zero, throwing an error if it is
- Handles both ascending (positive step) and descending (negative step) series
- Uses overflow-safe arithmetic to prevent infinite loops when the next value would overflow
- Maintains state across function calls using the multi_call_memory_ctx memory context
- Located in src/backend/utils/adt/int8.c:1383-1458
- Supports both 2-parameter (start, finish) and 3-parameter (start, finish, step) variants
- Part of PostgreSQL's comprehensive set-returning function family

## Simplified Source

```c
Datum
generate_series_step_int8(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    generate_series_fctx *fctx;
    int64 result;

    // First call: initialize the series
    if (SRF_IS_FIRSTCALL())
    {
        int64 start = PG_GETARG_INT64(0);
        int64 finish = PG_GETARG_INT64(1);
        int64 step = (PG_NARGS() == 3) ? PG_GETARG_INT64(2) : 1;

        // Validate step is not zero
        if (step == 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("step size cannot equal zero")));

        // Initialize function context
        funcctx = SRF_FIRSTCALL_INIT();

        // Allocate state structure
        fctx = (generate_series_fctx *) palloc(sizeof(generate_series_fctx));
        fctx->current = start;
        fctx->finish = finish;
        fctx->step = step;
        funcctx->user_fctx = fctx;
    }

    // Per-call setup
    funcctx = SRF_PERCALL_SETUP();
    fctx = funcctx->user_fctx;
    result = fctx->current;

    // Check if we should return more values
    if ((fctx->step > 0 && fctx->current <= fctx->finish) ||
        (fctx->step < 0 && fctx->current >= fctx->finish))
    {
        // Advance current value with overflow protection
        if (pg_add_s64_overflow(fctx->current, fctx->step, &fctx->current))
            fctx->step = 0;  // Stop on overflow

        SRF_RETURN_NEXT(funcctx, Int64GetDatum(result));
    }
    else
        SRF_RETURN_DONE(funcctx);
}
```