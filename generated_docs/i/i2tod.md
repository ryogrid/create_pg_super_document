# i2tod

## Location
src/backend/utils/adt/float.c: 1269 - 1280

## Overview
The i2tod function converts an int2 (smallint, 16-bit signed integer) to a float8 (double precision floating-point) number, providing a safe widening conversion with no precision loss.

## Definition
```c
Datum i2tod(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a straightforward type conversion in PostgreSQL's type system, converting 16-bit signed integers (smallint) to 64-bit double precision floating-point numbers. The conversion is always safe and lossless because:

1. The entire range of int16 (-32,768 to 32,767) fits comfortably within the range of float8
2. All smallint values can be exactly represented in double precision floating-point format without any rounding
3. No overflow, underflow, or precision loss can occur during this conversion

The function follows PostgreSQL's standard function calling convention, extracting the input parameter using the appropriate macro and returning the converted result. This represents a widening conversion that preserves exact numeric values while changing the internal representation.

## Parameters / Member Variables
- Input parameter (accessed via `PG_GETARG_INT16(0)`): The int2 (smallint) value to be converted to float8

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (PostgreSQL macro to extract int16 argument)
  - PG_RETURN_FLOAT8 (PostgreSQL macro to return float8 value)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This is a completely safe, lossless conversion since IEEE 754 double precision can exactly represent all 16-bit integer values
- The function is highly efficient, requiring only a simple cast operation
- Part of PostgreSQL's comprehensive type conversion system in src/backend/utils/adt/float.c
- Follows PostgreSQL's version-1 calling convention for built-in functions
- No error handling or range validation is necessary since the conversion cannot fail
- Commonly used when smallint values need to participate in floating-point arithmetic operations
- The converted values retain their exact mathematical value in the new floating-point representation