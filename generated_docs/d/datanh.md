# datanh

## Location
[src/backend/utils/adt/float.c:2707-2744](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2707-L2744)

## Overview
The datanh function computes the inverse hyperbolic tangent of a floating-point number with comprehensive domain validation and boundary case handling, providing PostgreSQL-specific error handling for mathematical operations.

## Definition
Datum datanh(PG_FUNCTION_ARGS)

## Detailed Description
The datanh function is a PostgreSQL wrapper around the standard C library atanh() function that calculates the inverse hyperbolic tangent (also known as area hyperbolic tangent) of a given floating-point argument. The function includes explicit domain validation since atanh is only mathematically defined for inputs in the range (-1, 1). The function explicitly handles boundary cases where the input equals -1.0 or 1.0, returning negative and positive infinity respectively, rather than relying on system library behavior which can be inconsistent across implementations. By performing explicit validation and boundary handling, PostgreSQL avoids relying on system-specific EDOM error handling and ensures consistent behavior across different platforms.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function call context containing the input argument
- arg1: The input float8 value for which to compute the inverse hyperbolic tangent (must be in range (-1, 1))
- result: The computed inverse hyperbolic tangent result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call context
  - [get_float8_infinity](../g/get_float8_infinity.md): Returns positive infinity representation (used twice for boundary cases)
  - ereport: Reports PostgreSQL errors
  - [errcode](../e/errcode.md): Sets error code for out of range values
  - [errmsg](../e/errmsg.md): Sets error message text
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function explicitly validates that -1.0 < input < 1.0 since atanh is undefined outside this range
- Special handling for boundary cases: arg1 == -1.0 returns negative infinity, arg1 == 1.0 returns positive infinity
- Domain validation prevents reliance on inconsistent system EDOM error handling across platforms
- Explicit boundary handling addresses potential issues with older glibc versions
- The function is part of PostgreSQL mathematical function library in src/backend/utils/adt/float.c
- Located at src/backend/utils/adt/float.c:2707-2744
- Error code ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE is used for invalid domain inputs

## Simplified Source

```c
Datum datanh(PG_FUNCTION_ARGS) {
    // Get input float8 value
    float8 arg1 = PG_GETARG_FLOAT8(0);

    // Validate domain: atanh is only defined for inputs in range (-1, 1)
    if (arg1 < -1.0 || arg1 > 1.0) {
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("input is out of range")));
    }

    // Handle boundary cases explicitly for consistent behavior
    float8 result;
    if (arg1 == -1.0) {
        result = -get_float8_infinity();
    } else if (arg1 == 1.0) {
        result = get_float8_infinity();
    } else {
        result = atanh(arg1);
    }

    PG_RETURN_FLOAT8(result);
}
```