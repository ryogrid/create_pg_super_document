# i4tod

## Location
src/backend/utils/adt/float.c: 1257 - 1268

## Overview
The i4tod function converts an int4 (32-bit signed integer) to a float8 (double precision floating-point) number, providing a straightforward widening conversion without precision loss.

## Definition
```c
Datum i4tod(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a simple but important type conversion in PostgreSQL's type system, converting 32-bit signed integers to 64-bit double precision floating-point numbers. The conversion is always safe and never results in data loss since:

1. The range of int32 (-2,147,483,648 to 2,147,483,647) fits entirely within the range representable by float8
2. All int32 values can be exactly represented in double precision floating-point format
3. No rounding or truncation is required

The function follows PostgreSQL's standard function interface, extracting the input parameter and returning the converted result using the appropriate macros. This is a widening conversion that preserves the exact numeric value while changing the data type representation.

## Parameters / Member Variables
- Input parameter (accessed via `PG_GETARG_INT32(0)`): The int4 value to be converted to float8

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (PostgreSQL macro to extract int32 argument)
  - PG_RETURN_FLOAT8 (PostgreSQL macro to return float8 value)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This is a loss-less conversion since all 32-bit integer values can be exactly represented in IEEE 754 double precision format
- The function is very simple and efficient, requiring only a direct cast operation
- Part of PostgreSQL's comprehensive type conversion system located in src/backend/utils/adt/float.c
- The function signature follows PostgreSQL's version-1 calling convention for built-in functions
- No range checking or error handling is needed since the conversion is always valid
- Commonly used in mathematical operations where integer operands need to be promoted to floating-point for calculation