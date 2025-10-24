# int2_sum

## Location
[src/backend/utils/adt/numeric.c:6524-6572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6524-L6572)

## Overview
A SQL aggregate transition function that computes the sum of smallint (int2) values, using int8 accumulator to prevent overflow.

## Definition
```c
Datum int2_sum(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the state transition function for the SUM() aggregate when applied to smallint (int2) data. To prevent overflow that could occur with int2 arithmetic, it uses a wider int8 (64-bit) accumulator. The function handles the SQL requirement that SUM() of no values returns NULL by explicitly managing null states.

The function is designed to be non-strict, meaning it must handle null inputs explicitly rather than relying on PostgreSQL's automatic null handling. When called in aggregate context, it can optimize performance by modifying the accumulator in-place rather than allocating new memory.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing:
  - Argument 0: Current accumulator state (int8*, initially NULL)
  - Argument 1: New int2 value to add to sum

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL (null checking macros)
  - PG_GETARG_INT16 (extract int2 argument)
  - PG_GETARG_INT64 (extract int8 argument)
  - PG_GETARG_POINTER (extract pointer argument)
  - PG_RETURN_NULL (return null result)
  - PG_RETURN_INT64 (return int8 result)
  - PG_RETURN_POINTER (return pointer result)
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (check if called in aggregate context)
- Called from:
  - No direct references found (called through PostgreSQL's aggregate mechanism)

## Notes and Other Information
- Part of PostgreSQL's integer sum aggregation system located in numeric.c despite being for integer types
- Uses conditional compilation (USE_FLOAT8_BYVAL) to optimize for different architectures
- The in-place modification optimization is only used when int8 is pass-by-reference
- Handles the first non-null input specially since the initial state is NULL
- Only used in plain aggregation mode; moving-aggregate mode uses different functions (intX_avg_accum and intX_avg_accum_inv)
- Located in src/backend/utils/adt/numeric.c:6524-6572

## Simplified Source

```c
Datum int2_sum(PG_FUNCTION_ARGS) {
    int64 newval;

    // Handle first non-null input (initial state is NULL)
    if (PG_ARGISNULL(0)) {
        if (PG_ARGISNULL(1))
            PG_RETURN_NULL();  // No non-null values yet

        // First non-null input becomes initial accumulator value
        newval = (int64) PG_GETARG_INT16(1);
        PG_RETURN_INT64(newval);
    }

    // Optimize for aggregate context: modify accumulator in-place
    if (AggCheckCallContext(fcinfo, NULL)) {
        int64 *oldsum = (int64 *) PG_GETARG_POINTER(0);

        // Add new value to running sum (skip if null)
        if (!PG_ARGISNULL(1))
            *oldsum = *oldsum + (int64) PG_GETARG_INT16(1);

        PG_RETURN_POINTER(oldsum);
    }

    // Non-aggregate context: return new sum value
    int64 oldsum = PG_GETARG_INT64(0);

    if (PG_ARGISNULL(1))
        PG_RETURN_INT64(oldsum);  // Skip null inputs

    newval = oldsum + (int64) PG_GETARG_INT16(1);
    PG_RETURN_INT64(newval);
}
```