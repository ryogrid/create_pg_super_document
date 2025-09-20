# in_range_float8_float8

## Location
[src/backend/utils/adt/float.c:1020-1095](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1020-L1095)

## Overview
A support function for window frame range calculations with float8 (double precision) values that determines if a given value falls within a specified range relative to a base value.

## Definition

```c
Datum
in_range_float8_float8(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL in_range support for float8 data types, used primarily in window functions with RANGE frames. It determines whether a value falls within a range defined by a base value plus or minus an offset. The function handles special floating-point cases including NaN and infinity values according to PostgreSQL's sorting semantics where NaN sorts after all non-NaN values.

The function performs range checking by computing base +/- offset and comparing the result with the input value. It includes comprehensive error handling for invalid offset values and special logic for edge cases involving infinite and NaN values.

## Parameters / Member Variables
-  (float8): The value to test for inclusion in the range
-  (float8): The base value that defines the center of the range
-  (float8): The distance from base that defines the range boundary (must be non-negative and non-NaN)
-  (bool): If true, compute base - offset; if false, compute base + offset
-  (bool): If true, test val <= boundary; if false, test val >= boundary

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (argument extraction macro)
  - PG_GETARG_BOOL (boolean argument extraction macro)
  - isnan (NaN detection)
  - isinf (infinity detection)
  - ereport (error reporting)
  - PG_RETURN_BOOL (boolean return macro)
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's function dispatch mechanism)

## Notes and Other Information
- This function is specifically designed for window function RANGE frames with float8 precision
- Rejects negative or NaN offset values with ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE
- Implements PostgreSQL's NaN sorting semantics (NaN > all non-NaN values)
- Handles infinite base and offset combinations that would produce NaN results
- Does not require a float8_float4 variant as implicit coercion handles mixed precision scenarios
- Source location: src/backend/utils/adt/float.c:1020-1095