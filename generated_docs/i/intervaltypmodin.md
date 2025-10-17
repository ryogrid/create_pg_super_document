# intervaltypmodin

## Location
[src/backend/utils/adt/timestamp.c:1056-1134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1056-L1134)

## Overview
Validates and constructs type modifier values for the INTERVAL data type, processing precision and field range parameters to create a compressed typmod representation.

## Definition

```c
Datum
intervaltypmodin(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is responsible for processing the type modifier parameters for PostgreSQL's INTERVAL data type. It takes an array of integer type modifiers and validates them according to SQL standard interval specifications, then encodes them into a single 32-bit typmod value.

The function handles two key aspects of interval type specification:
1. **Range (field mask)**: A bitmap specifying which temporal fields (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND) are permitted
2. **Precision**: Sub-second decimal precision specification

The typmod encoding stores the range in the high 16 bits and precision in the low 16 bits, allowing efficient representation of all SQL standard interval resolutions while supporting PostgreSQL's truncation-based implementation.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : ArrayType pointer containing the type modifier array
  - : Integer array extracted from the ArrayType
  - : Number of elements in the type modifier array

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P
  - [ArrayGetIntegerTypmods](../A/ArrayGetIntegerTypmods.md)
  - INTERVAL_MASK
  - INTERVAL_FULL_RANGE
  - INTERVAL_FULL_PRECISION
  - INTERVAL_TYPMOD
  - MAX_INTERVAL_PRECISION
  - ereport/errcode/errmsg (error reporting)
- Called from (representative examples):
  - SQL type system during INTERVAL type declaration
  - Parser during interval type specification processing

## Notes and Other Information
- Validates specific combinations of interval fields according to SQL standard (e.g., YEAR TO MONTH, DAY TO SECOND)
- Supports precision values from 0 to MAX_INTERVAL_PRECISION, with automatic clamping and warnings
- Returns -1 for full range intervals without explicit precision
- Part of PostgreSQL's type system infrastructure for interval data type management
- Handles error cases gracefully with descriptive error messages for invalid type modifiers

## Simplified Source

```c
Datum intervaltypmodin(PG_FUNCTION_ARGS) {
    ArrayType *ta = PG_GETARG_ARRAYTYPE_P(0);
    int32 *tl;
    int n;
    int32 typmod;

    // Extract integer type modifiers from array
    tl = ArrayGetIntegerTypmods(ta, &n);

    // Validate field range specification (tl[0])
    if (n > 0) {
        // Check against valid interval field combinations
        switch (tl[0]) {
            case INTERVAL_MASK(YEAR):
            case INTERVAL_MASK(MONTH):
            case INTERVAL_MASK(DAY):
            case INTERVAL_MASK(HOUR):
            case INTERVAL_MASK(MINUTE):
            case INTERVAL_MASK(SECOND):
            case INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH):
            case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR):
            // ... other valid combinations
            case INTERVAL_FULL_RANGE:
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("invalid INTERVAL type modifier")));
        }
    }

    // Process based on number of modifiers
    if (n == 1) {
        // Only range specified, use full precision
        if (tl[0] != INTERVAL_FULL_RANGE)
            typmod = INTERVAL_TYPMOD(INTERVAL_FULL_PRECISION, tl[0]);
        else
            typmod = -1;
    } else if (n == 2) {
        // Both range and precision specified
        if (tl[1] < 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("INTERVAL(%d) precision must not be negative", tl[1])));

        if (tl[1] > MAX_INTERVAL_PRECISION) {
            ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("INTERVAL(%d) precision reduced to maximum allowed, %d",
                    tl[1], MAX_INTERVAL_PRECISION)));
            typmod = INTERVAL_TYPMOD(MAX_INTERVAL_PRECISION, tl[0]);
        } else {
            typmod = INTERVAL_TYPMOD(tl[1], tl[0]);
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid INTERVAL type modifier")));
        typmod = 0;
    }

    return PG_RETURN_INT32(typmod);
}
```