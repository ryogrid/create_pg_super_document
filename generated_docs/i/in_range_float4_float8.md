# in_range_float4_float8

## Location
[src/backend/utils/adt/float.c:1096-1175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1096-L1175)

## Overview
A support function for window frame range calculations with float4 (single precision) values that determines if a given value falls within a specified range using a float8 offset for precision.

## Definition

```c
Datum
in_range_float4_float8(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL in_range support for float4 data types with float8 precision offsets, used primarily in window functions with RANGE frames. It determines whether a float4 value falls within a range defined by a float4 base value plus or minus a float8 offset. The mixed precision design allows for more precise offset calculations while working with single-precision base values.

Like its float8 counterpart, this function handles special floating-point cases including NaN and infinity values according to PostgreSQL's sorting semantics. The function performs range checking by computing base +/- offset in float8 precision and comparing the result with the input value.

## Parameters / Member Variables
-  (float4): The single-precision value to test for inclusion in the range
-  (float4): The single-precision base value that defines the center of the range
-  (float8): The double-precision distance from base that defines the range boundary (must be non-negative and non-NaN)
-  (bool): If true, compute base - offset; if false, compute base + offset
-  (bool): If true, test val <= boundary; if false, test val >= boundary

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4 (single-precision argument extraction macro)
  - PG_GETARG_FLOAT8 (double-precision argument extraction macro)
  - PG_GETARG_BOOL (boolean argument extraction macro)
  - isnan (NaN detection)
  - isinf (infinity detection)
  - ereport (error reporting)
  - PG_RETURN_BOOL (boolean return macro)
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's function dispatch mechanism)

## Notes and Other Information
- This function provides float4_float8 variant while letting implicit coercion handle float4_float4 cases
- Uses mixed precision: float4 for val and base, float8 for offset and internal calculations
- Rejects negative or NaN offset values with ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE
- Implements PostgreSQL's NaN sorting semantics (NaN > all non-NaN values)
- Handles infinite base and offset combinations that would produce NaN results
- The higher precision offset allows for more accurate range calculations with single-precision data
- Source location: src/backend/utils/adt/float.c:1096-1175

## Simplified Source

```c
Datum in_range_float4_float8(PG_FUNCTION_ARGS) {
    // Extract arguments: float4 value/base, float8 offset, boolean flags
    float4 val = PG_GETARG_FLOAT4(0);
    float4 base = PG_GETARG_FLOAT4(1);
    float8 offset = PG_GETARG_FLOAT8(2);
    bool sub = PG_GETARG_BOOL(3);
    bool less = PG_GETARG_BOOL(4);

    // Validate offset: must be non-negative and not NaN
    if (isnan(offset) || offset < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
                       errmsg("invalid preceding or following size in window function")));

    // Handle NaN cases: NaN sorts after all non-NaN values
    if (isnan(val)) {
        if (isnan(base))
            PG_RETURN_BOOL(true);  // NaN = NaN
        else
            PG_RETURN_BOOL(!less); // NaN > non-NaN
    }
    else if (isnan(base)) {
        PG_RETURN_BOOL(less);      // non-NaN < NaN
    }

    // Handle infinite base/offset edge case
    if (isinf(offset) && isinf(base) && (sub ? base > 0 : base < 0))
        PG_RETURN_BOOL(true);

    // Compute range boundary in float8 precision: base +/- offset
    float8 sum = sub ? base - offset : base + offset;

    // Compare value with boundary
    PG_RETURN_BOOL(less ? val <= sum : val >= sum);
}
```