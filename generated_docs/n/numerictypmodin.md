# numerictypmodin

## Location
[src/backend/utils/adt/numeric.c:1322-1366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1322-L1366)

## Overview
The  function parses and validates type modifier strings for the NUMERIC data type, converting them into internal typmod representations.

## Definition

```c
Datum
numerictypmodin(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is part of PostgreSQL's type modifier input system for the NUMERIC type. It takes an array of type modifier values (typically precision and scale) from a type declaration like NUMERIC(10,2) and converts them into a single int32 typmod value used internally by the system. The function validates that precision and scale values are within acceptable ranges and handles both single-parameter (precision only) and dual-parameter (precision and scale) formats.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Array of type modifier values from the type declaration (PG_GETARG_ARRAYTYPE_P(0))
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P: Extracts array argument from function call
  - [ArrayGetIntegerTypmods](../A/ArrayGetIntegerTypmods.md): Extracts integer values from the typmod array
  - NUMERIC_MAX_PRECISION: Maximum allowed precision constant
  - NUMERIC_MIN_SCALE: Minimum allowed scale constant  
  - NUMERIC_MAX_SCALE: Maximum allowed scale constant
  - ereport: Error reporting function
  - [errcode](../e/errcode.md): Error code specification
  - [errmsg](../e/errmsg.md): Error message formatting
  - [make_numeric_typmod](../m/make_numeric_typmod.md): Creates internal typmod representation
  - PG_RETURN_INT32: Returns int32 result

- Called from (representative examples):
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md): JSON path execution context

## Notes and Other Information
- Handles type declarations like NUMERIC(precision) and NUMERIC(precision, scale)
- Enforces PostgreSQL's limits on numeric precision and scale values
- Scale defaults to 0 when only precision is specified
- Validates precision must be between 1 and NUMERIC_MAX_PRECISION
- Validates scale must be between NUMERIC_MIN_SCALE and NUMERIC_MAX_SCALE
- Part of the type system's input/output machinery for custom types
- Located in src/backend/utils/adt/numeric.c:1322-1366

## Simplified Source

```c
Datum
numerictypmodin(PG_FUNCTION_ARGS)
{
    ArrayType *ta = PG_GETARG_ARRAYTYPE_P(0);
    int32 *tl;
    int n;
    int32 typmod;

    // Extract integer values from type modifier array
    tl = ArrayGetIntegerTypmods(ta, &n);

    if (n == 2) {
        // NUMERIC(precision, scale) format
        if (tl[0] < 1 || tl[0] > NUMERIC_MAX_PRECISION)
            ereport(ERROR, "NUMERIC precision %d must be between 1 and %d");
        if (tl[1] < NUMERIC_MIN_SCALE || tl[1] > NUMERIC_MAX_SCALE)
            ereport(ERROR, "NUMERIC scale %d must be between %d and %d");

        typmod = make_numeric_typmod(tl[0], tl[1]);
    }
    else if (n == 1) {
        // NUMERIC(precision) format - scale defaults to 0
        if (tl[0] < 1 || tl[0] > NUMERIC_MAX_PRECISION)
            ereport(ERROR, "NUMERIC precision %d must be between 1 and %d");

        typmod = make_numeric_typmod(tl[0], 0);
    }
    else {
        // Invalid number of type modifiers
        ereport(ERROR, "invalid NUMERIC type modifier");
        typmod = 0;  // keep compiler quiet
    }

    PG_RETURN_INT32(typmod);
}
```