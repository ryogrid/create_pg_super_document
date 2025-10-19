# int2mod

## Location
[src/backend/utils/adt/int.c:1158-1190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1158-L1190)

## Overview
Computes the modulo (remainder) of two 16-bit integers, handling division-by-zero and the special case of any value % -1.

## Definition
```c
Datum int2mod(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int2mod` function implements the modulo operation between two 16-bit integers (int2), returning the remainder as a 16-bit integer. The function handles edge cases including division by zero (raises ERRCODE_DIVISION_BY_ZERO) and the special case of any number modulo -1, which always returns 0. Although floating-point exceptions for INT_MIN % -1 are less likely with 16-bit integers, the function includes this check for safety and consistency with other modulo implementations.

## Parameters / Member Variables
- `arg1`: The 16-bit integer dividend (number being divided)
- `arg2`: The 16-bit integer divisor (number dividing by)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT16`: Extracts both 16-bit integer arguments
  - `ereport`: Reports division by zero errors
  - `PG_RETURN_INT16`: Returns the 16-bit result
  - `PG_RETURN_NULL`: Used as unreachable code marker after division by zero error
- Called from: No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's arithmetic operator implementation for 16-bit integers
- The function follows PostgreSQL's function call convention using PG_FUNCTION_ARGS
- Division by zero is explicitly checked and reported as an error
- The special case of modulo -1 is handled for consistency and safety, even though it's less critical for 16-bit values
- Modulo -1 always mathematically equals 0, which is returned directly
- No general overflow checking is needed for modulo operations
- The function includes a compiler hint (PG_RETURN_NULL after division by zero error) to help with optimization
- This implementation mirrors the int4mod function but operates on 16-bit values

## Simplified Source

```c
Datum int2mod(PG_FUNCTION_ARGS) {
    int16 dividend = PG_GETARG_INT16(0);  // First operand
    int16 divisor = PG_GETARG_INT16(1);   // Second operand

    // Check for division by zero
    if (divisor == 0) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                       errmsg("division by zero")));
    }

    // Handle special case: any number % -1 = 0
    if (divisor == -1) {
        PG_RETURN_INT16(0);
    }

    // Perform modulo operation
    PG_RETURN_INT16(dividend % divisor);
}
```