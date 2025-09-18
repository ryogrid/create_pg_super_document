# ParseISO8601Number

## Location
[src/interfaces/ecpg/pgtypeslib/interval.c:56-80](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/interval.c#L56-L80)

## Overview
ParseISO8601Number is a helper function that parses a decimal value from a string and breaks it into integer and fractional parts, specifically designed for ISO 8601 interval parsing.

## Definition


## Detailed Description
This function serves as a crucial component in PostgreSQL's ISO 8601 interval parsing system. It accepts various numeric formats that strtod() would accept, including scientific notation, but applies additional validation and constraints specific to PostgreSQL's requirements. The function ensures precise separation of integer and fractional parts while maintaining compatibility with historical behavior.

Key design considerations include:
- Accepts inputs that strtod() would process, including scientific notation for backward compatibility
- Limits input range to prevent precision loss from double to int64 conversion
- Rejects values with absolute value above 1.0e15 to ensure exact integer representation
- Guarantees the fractional part has absolute value less than 1.0
- Uses careful truncation toward zero to match PostgreSQL's dtrunc() behavior

The function validates input format, performs range checking, extracts integer and fractional components, and returns appropriate error codes for various failure conditions.

## Parameters / Member Variables
- : Input string containing the number to parse
- : Output pointer that will be set to the character after the parsed number
- : Output pointer for the integer part of the parsed number (int64)
- : Output pointer for the fractional part of the parsed number (double, |fpart| < 1.0)

## Dependencies
- Functions called/Symbols referenced:
  - DTERR_BAD_FORMAT (error code for malformed input)
  - DTERR_FIELD_OVERFLOW (error code for values outside acceptable range)
  - isnan (standard math function to check for NaN)
  - strtod (standard library function for string to double conversion)
  - floor (standard math function for floor operation)
- Called from (representative examples):
  - [DecodeISO8601Interval](../D/DecodeISO8601Interval.md) (multiple locations in backend and ECPG)

## Notes and Other Information
- This is a static helper function within src/backend/utils/adt/datetime.c
- Returns 0 on success, or DTERR error codes on failure
- Maintains historical compatibility by accepting scientific notation despite potential precision concerns
- The 1.0e15 limit ensures that any accepted value will have an exact integer part when stored as int64
- Uses truncation toward zero (not standard rounding) to match PostgreSQL's dtrunc() function behavior
- Includes an assertion to verify the fractional part constraint for debugging builds
- There is also an ECPG version in src/interfaces/ecpg/pgtypeslib/interval.c with similar functionality but slightly different parameter types
- Part of the broader ISO 8601 interval parsing infrastructure in PostgreSQL