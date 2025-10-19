# i2tof

## Location
[src/backend/utils/adt/float.c:1343-1360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1343-L1360)

## Overview
Converts a 16-bit integer (int2) to a single-precision floating-point number (float4).

## Definition
```c
Datum i2tof(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL built-in function for converting int2 (16-bit integer) values to float4 (single-precision floating-point) values. It follows the standard PostgreSQL function calling convention using the fmgr interface, taking arguments via PG_FUNCTION_ARGS and returning a Datum. The conversion is performed using a simple C cast from int16 to float4.

## Parameters / Member Variables
- Takes one argument accessed via `PG_GETARG_INT16(0)`: The int2 value to be converted to float4

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (macro for retrieving int16 argument)
  - PG_RETURN_FLOAT4 (macro for returning float4 result)
  - float4 (typedef for single-precision float)

- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:1343-1360
- This is a straightforward type conversion function that relies on C's built-in casting
- The conversion is lossless since int2 values (16-bit integers) can be exactly represented in float4 (single-precision floating-point)
- Part of PostgreSQL's type conversion system for numeric types
- Similar to i4tof but operates on smaller integer values (int2 vs int4)

## Simplified Source

```c
Datum i2tof(PG_FUNCTION_ARGS) {
    int16 num = PG_GETARG_INT16(0);

    // Simple cast from int16 to float4
    PG_RETURN_FLOAT4((float4) num);
}
```