# generate_series_timestamp

## Location
[src/backend/utils/adt/timestamp.c:6506-6589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L6506-L6589)

## Overview
This function generates a set-returning series of timestamp values from a start timestamp to a finish timestamp, incrementing by a specified interval step, implementing PostgreSQL's generate_series() function for timestamp data types.

## Definition

```c
Datum
generate_series_timestamp(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a set-returning function (SRF) that produces a sequence of timestamp values between two bounds. It follows PostgreSQL's standard SRF pattern with initialization on the first call and state management across multiple calls.

**First Call Initialization:**
- Extracts start timestamp, finish timestamp, and step interval from function arguments
- Validates that the step is non-zero and finite
- Determines the sign of the interval to control iteration direction
- Sets up persistent memory context and function state structure
- Initializes the current position to the start value

**Subsequent Calls:**
- Compares current timestamp with finish timestamp based on step direction
- If more values remain: returns current timestamp and advances to next position
- If series complete: signals completion to PostgreSQL's SRF framework

The function handles both forward (positive step) and backward (negative step) iteration, automatically determining the comparison logic based on the interval sign. It uses  for timestamp arithmetic and  for comparisons.

## Parameters / Member Variables
- Argument 0:  (Timestamp) - The starting timestamp of the series
- Argument 1:  (Timestamp) - The ending timestamp of the series  
- Argument 2:  (Interval*) - The interval increment between consecutive timestamps

## Dependencies
- Functions called/Symbols referenced:
  -  - checks if this is the first function call
  -  - retrieves timestamp arguments
  -  - retrieves interval argument
  -  - initializes set-returning function context
  -  - manages memory contexts
  -  - allocates memory in current context
  -  - determines the sign/direction of an interval
  -  - checks for infinite interval values
  -  - sets up context for each function call
  -  - compares two timestamp values
  -  - calls PostgreSQL functions
  -  - adds interval to timestamp
  -  /  - datum conversion functions
  -  - converts pointer to datum
  -  - returns next value in series
  -  - signals series completion
- Called from:
  - No direct references found (called via SQL function dispatch)

## Notes and Other Information
- This function implements the SQL  function
- Uses PostgreSQL's set-returning function framework for efficient streaming of results
- Supports both forward and backward iteration based on interval sign
- Maintains state across multiple calls using the  structure
- Located in  at lines 6506-6589
- Validates input parameters to prevent infinite loops (zero step) and invalid operations (infinite step)
- Memory management follows PostgreSQL's multi-call function pattern with appropriate context switching
- The function can generate arbitrarily long sequences limited only by timestamp range and available memory
- Comparison logic automatically adapts to step direction: <= for positive steps, >= for negative steps
- Used extensively in PostgreSQL for generating timestamp sequences in queries and reports

## Simplified Source

```c
Datum generate_series_timestamp(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    generate_series_timestamp_fctx *fctx;
    Timestamp result;

    // First call: initialize state
    if (SRF_IS_FIRSTCALL()) {
        Timestamp start = PG_GETARG_TIMESTAMP(0);
        Timestamp finish = PG_GETARG_TIMESTAMP(1);
        Interval *step = PG_GETARG_INTERVAL_P(2);
        MemoryContext oldcontext;

        // Set up function context for multi-call persistence
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        // Allocate and initialize state structure
        fctx = (generate_series_timestamp_fctx *)
               palloc(sizeof(generate_series_timestamp_fctx));

        fctx->current = start;
        fctx->finish = finish;
        fctx->step = *step;

        // Determine iteration direction based on interval sign
        fctx->step_sign = interval_sign(&fctx->step);

        // Validate step parameters
        if (fctx->step_sign == 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("step size cannot equal zero")));

        if (INTERVAL_NOT_FINITE((&fctx->step)))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("step size cannot be infinite")));

        funcctx->user_fctx = fctx;
        MemoryContextSwitchTo(oldcontext);
    }

    // Subsequent calls: return next value or finish
    funcctx = SRF_PERCALL_SETUP();
    fctx = funcctx->user_fctx;
    result = fctx->current;

    // Check if more values to return (direction-dependent comparison)
    if (fctx->step_sign > 0 ?
        timestamp_cmp_internal(result, fctx->finish) <= 0 :
        timestamp_cmp_internal(result, fctx->finish) >= 0) {

        // Advance to next timestamp
        fctx->current = DatumGetTimestamp(DirectFunctionCall2(timestamp_pl_interval,
                                          TimestampGetDatum(fctx->current),
                                          PointerGetDatum(&fctx->step)));

        // Return current value
        SRF_RETURN_NEXT(funcctx, TimestampGetDatum(result));
    } else {
        // Series complete
        SRF_RETURN_DONE(funcctx);
    }
}
```