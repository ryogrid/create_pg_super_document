# int2_accum_inv

## Location
[src/backend/utils/adt/numeric.c:5990-6014](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L5990-L6014)

## Overview
Inverse transition function for int2 (smallint) input that removes values from the aggregate state, enabling sliding window aggregations.

## Definition
```c
Datum int2_accum_inv(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the inverse operation for int2 accumulation, removing a value from an existing PolyNumAggState. It's designed for use in sliding window aggregates and moving averages where values need to be removed from the running calculation as the window moves. The function supports both 128-bit integer arithmetic (when available) and falls back to numeric arithmetic.

The function performs validation to ensure it's not called with a NULL state, as inverse operations require an existing accumulation state. It handles NULL input values by ignoring them, consistent with SQL aggregate behavior.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function calling convention macro that provides access to:
  - Arg 0: PolyNumAggState pointer (existing accumulation state, must not be NULL)
  - Arg 1: int16 value to remove from accumulation (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - `PolyNumAggState` (structure type)
  - [do_int128_discard](../d/do_int128_discard.md) (removes value from 128-bit integer accumulation when HAVE_INT128)
  - [do_numeric_discard](../d/do_numeric_discard.md) (removes value from numeric accumulation)
  - [int64_to_numeric](int64_to_numeric.md) (converts int64 to numeric type)
  - `PG_GETARG_INT16` (extracts int16 argument)
- Called from (representative examples):
  - No direct references found (likely referenced through PostgreSQL's sliding window aggregate system)

## Notes and Other Information
- Critical for sliding window aggregations and moving averages
- Requires existing state (NULL state triggers error)
- Uses conditional compilation with HAVE_INT128 for optimized arithmetic
- Input values have dscale 0 (no decimal places) as they are integers
- Part of PostgreSQL's inverse aggregate function framework for windowing operations
- Error handling includes validation that numeric discard operations succeed as expected

## Simplified Source

```c
Datum
int2_accum_inv(PG_FUNCTION_ARGS)
{
    PolyNumAggState *state;

    state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

    // Validate state exists
    if (state == NULL)
        elog(ERROR, "int2_accum_inv called with NULL state");

    // Remove non-NULL input values from accumulation
    if (!PG_ARGISNULL(1))
    {
        // Use 128-bit arithmetic if available, otherwise numeric
#ifdef HAVE_INT128
        do_int128_discard(state, (int128) PG_GETARG_INT16(1));
#else
        if (!do_numeric_discard(state, int64_to_numeric(PG_GETARG_INT16(1))))
            elog(ERROR, "do_numeric_discard failed unexpectedly");
#endif
    }

    return state;
}
```