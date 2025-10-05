# generate_series_step_int4

## Location
[src/backend/utils/adt/int.c:1509-1584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1509-L1584)

## Overview
Implements the core logic for generating a series of 32-bit integers with configurable start, finish, and step values as a set-returning function.

## Definition
```c
Datum generate_series_step_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
The `generate_series_step_int4` function is the main implementation of PostgreSQL's `generate_series()` function for 32-bit integers. It generates a sequence of integers from a start value to a finish value, incrementing by a specified step size. The function uses PostgreSQL's Set-Returning Function (SRF) framework to return multiple rows across multiple calls. It maintains state between function calls using a function context structure, allowing it to remember the current position in the series. The function handles both positive and negative step values and includes overflow protection to prevent infinite loops.

## Parameters / Member Variables
- `start`: The 32-bit integer starting value of the series (from PG_GETARG_INT32(0))
- `finish`: The 32-bit integer ending value of the series (from PG_GETARG_INT32(1))
- `step`: The 32-bit integer step size (from PG_GETARG_INT32(2), defaults to 1 if not provided)
- `funcctx`: Function call context for maintaining state between calls
- `fctx`: User-defined context structure containing current, finish, and step values

## Dependencies
- Functions called/Symbols referenced:
  - `SRF_IS_FIRSTCALL` - Macro to check if this is the first function call
  - `PG_GETARG_INT32` - Macro to extract int32 arguments
  - `PG_NARGS` - Macro to get the number of arguments
  - `SRF_FIRSTCALL_INIT` - [Initialize](../I/Initialize.md) SRF context on first call
  - `SRF_PERCALL_SETUP` - Setup for each subsequent call
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) - Safe integer addition with overflow detection
  - `SRF_RETURN_NEXT` - Return next value in the series
  - `SRF_RETURN_DONE` - Signal end of series
  - `generate_series_fctx` - Context structure for maintaining state
  - [FuncCallContext](../F/FuncCallContext.md) - PostgreSQL function call context structure
- Called from (representative examples):
  - [generate_series_int4](generate_series_int4.md) - Wrapper function for two-parameter version

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:1509-1584`
- Validates that step size is not zero, throwing an error if it is
- Uses memory context switching for proper memory management across multiple calls
- Handles both ascending (positive step) and descending (negative step) series
- Includes overflow protection using `pg_add_s32_overflow` to prevent infinite loops
- Part of PostgreSQL's generate_series() SQL function family
- Can handle 2 or 3 parameters (start, finish, and optional step)

## Simplified Source

```c
Datum generate_series_step_int4(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    generate_series_fctx *fctx;
    int32 result;

    // First call: initialize the series
    if (SRF_IS_FIRSTCALL()) {
        int32 start = PG_GETARG_INT32(0);
        int32 finish = PG_GETARG_INT32(1);
        int32 step = (PG_NARGS() == 3) ? PG_GETARG_INT32(2) : 1;

        // Validate step is not zero
        if (step == 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("step size cannot equal zero")));

        // Initialize function context and memory
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // Allocate and initialize series state
        fctx = (generate_series_fctx *) palloc(sizeof(generate_series_fctx));
        fctx->current = start;
        fctx->finish = finish;
        fctx->step = step;
        funcctx->user_fctx = fctx;

        MemoryContextSwitchTo(oldcontext);
    }

    // Every call: get current state and generate next value
    funcctx = SRF_PERCALL_SETUP();
    fctx = funcctx->user_fctx;
    result = fctx->current;

    // Check if we should continue the series
    if ((fctx->step > 0 && fctx->current <= fctx->finish) ||
        (fctx->step < 0 && fctx->current >= fctx->finish)) {

        // Advance to next value, stop on overflow
        if (pg_add_s32_overflow(fctx->current, fctx->step, &fctx->current))
            fctx->step = 0;

        SRF_RETURN_NEXT(funcctx, Int32GetDatum(result));
    }
    else {
        SRF_RETURN_DONE(funcctx);
    }
}
```