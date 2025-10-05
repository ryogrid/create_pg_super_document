# in_range_int4_int2

## Location
[src/backend/utils/adt/int.c:657-668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L657-L668)

## Overview
A PostgreSQL function that determines whether a given int4 value falls within a range defined by a base int4 value and an int2 offset, serving as a wrapper for in_range_int4_int4.

## Definition
```c
Datum in_range_int4_int2(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides range-based comparison functionality for PostgreSQL window functions where the offset parameter is an int2 (16-bit integer) instead of int4. Rather than duplicating the core logic, it acts as a thin wrapper that converts the int2 offset to int4 and delegates the actual work to in_range_int4_int4 using DirectFunctionCall5. This design approach avoids code duplication while supporting cross-data-type range operations in window functions.

## Parameters / Member Variables
- `val`: The int4 value to test against the range (passed through unchanged)
- `base`: The int4 base value that defines the center of the range (passed through unchanged)
- `offset`: The int2 offset value that defines the range size (converted to int4)
- `sub`: Boolean flag indicating whether to subtract the offset (passed through unchanged)
- `less`: Boolean flag indicating the comparison direction (passed through unchanged)

## Dependencies
- Functions called/Symbols referenced:
  - [in_range_int4_int4](in_range_int4_int4.md) (main implementation function)
  - DirectFunctionCall5 (PostgreSQL direct function call mechanism)
  - PG_GETARG_DATUM (macro to extract datum arguments)
  - PG_GETARG_INT16 (macro to extract int2 argument)
  - [Int32GetDatum](../I/Int32GetDatum.md) (macro to convert int32 to Datum)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:657-668
- Part of PostgreSQL's window function range frame support
- Avoids code duplication by delegating to in_range_int4_int4
- Handles cross-data-type scenarios where offset is int2 but other values are int4
- Uses PostgreSQL's DirectFunctionCall5 mechanism for efficient function delegation
- The int2 offset is safely converted to int4 without overflow concerns since int2 range fits within int4
- Maintains the same error handling and overflow detection as the underlying in_range_int4_int4 function

## Simplified Source

```c
Datum in_range_int4_int2(PG_FUNCTION_ARGS) {
    // This is a wrapper function that converts int2 offset to int4
    // and delegates to in_range_int4_int4 to avoid code duplication

    return DirectFunctionCall5(in_range_int4_int4,
                              PG_GETARG_DATUM(0),    // val (int4)
                              PG_GETARG_DATUM(1),    // base (int4)
                              Int32GetDatum((int32) PG_GETARG_INT16(2)), // offset (int2→int4)
                              PG_GETARG_DATUM(3),    // sub (bool)
                              PG_GETARG_DATUM(4));   // less (bool)
}
```