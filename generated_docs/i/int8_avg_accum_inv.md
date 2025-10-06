# int8_avg_accum_inv

## Location
[src/backend/utils/adt/numeric.c:6061-6085](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6061-L6085)

## Overview
Inverse accumulation function specifically for 64-bit integer average aggregate operations that removes a value from the running aggregate state.

## Definition
```c
Datum int8_avg_accum_inv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int8_avg_accum_inv` function serves as the inverse accumulation function for average aggregate operations involving 64-bit integers. It's designed for use in window functions and moving averages where values need to be removed from the running state as the window slides. The function takes a `PolyNumAggState` pointer and a 64-bit integer value to remove from the accumulation state.

This function is similar to `int4_accum_inv` in that it uses conditional compilation to optimize for platforms with 128-bit integer support, but differs from `int8_accum_inv` by using `PolyNumAggState` instead of `NumericAggState`, making it suitable for polymorphic aggregate operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `state`: PolyNumAggState pointer representing the current aggregate state
  - `value`: int64 value to be removed from the accumulation

## Dependencies
- Functions called/Symbols referenced:
  - `PolyNumAggState` (polymorphic aggregate state structure)
  - [do_int128_discard](../d/do_int128_discard.md) (128-bit integer discard operation)
  - [do_numeric_discard](../d/do_numeric_discard.md) (numeric discard operation)
  - [int64_to_numeric](int64_to_numeric.md) (conversion function)
  - `PG_GETARG_INT64` (argument extraction macro)
  - `HAVE_INT128` (compilation flag)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Uses `PolyNumAggState` for polymorphic aggregate support, unlike `int8_accum_inv`
- Includes conditional compilation with HAVE_INT128 for optimized 128-bit arithmetic
- Specifically designed for average operations rather than general accumulation
- Part of PostgreSQL's aggregate function framework for window functions and moving aggregates
- Located in src/backend/utils/adt/numeric.c:6061-6085

## Simplified Source

```c
Datum
int8_avg_accum_inv(PG_FUNCTION_ARGS)
{
    PolyNumAggState *state;

    state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

    // Validate state exists
    if (state == NULL)
        elog(ERROR, "int8_avg_accum_inv called with NULL state");

    // Remove non-NULL input values from accumulation
    if (!PG_ARGISNULL(1))
    {
        // Use 128-bit arithmetic if available, otherwise numeric
#ifdef HAVE_INT128
        do_int128_discard(state, (int128) PG_GETARG_INT64(1));
#else
        if (!do_numeric_discard(state, int64_to_numeric(PG_GETARG_INT64(1))))
            elog(ERROR, "do_numeric_discard failed unexpectedly");
#endif
    }

    return state;
}
```