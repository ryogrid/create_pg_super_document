# tsrange_subdiff

## Location
src/backend/utils/adt/rangetypes.c: 1664 - 1674

## Overview
Computes the difference between two timestamp values for use in timestamp range type operations, returning the result as a float8 value representing the number of seconds.

## Definition
Datum tsrange_subdiff(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the subdiff function for tsrange (timestamp without time zone range) types in PostgreSQL. It takes two timestamp values and computes their difference in seconds, returning the result as a float8 value. PostgreSQL stores timestamps internally as microseconds since the PostgreSQL epoch (January 1, 2000), so the function converts the microsecond difference to seconds by dividing by USECS_PER_SEC.

The function performs arithmetic subtraction on the timestamp values (which are stored as int64 microsecond counts) and then divides by USECS_PER_SEC (1000000) to convert from microseconds to seconds. This provides a meaningful time interval measurement in the standard unit of seconds.

## Parameters / Member Variables
- v1: First timestamp value (Timestamp/int64) - microseconds since PostgreSQL epoch, the minuend in the subtraction
- v2: Second timestamp value (Timestamp/int64) - microseconds since PostgreSQL epoch, the subtrahend in the subtraction  
- result: Calculated difference in seconds (float8)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP (macro for extracting Timestamp arguments)
  - USECS_PER_SEC (constant defining microseconds per second, value 1000000)
  - PG_RETURN_FLOAT8 (macro for returning float8 values)
- Type definitions:
  - Timestamp (PostgreSQL timestamp type, typically int64)

## Notes and Other Information
- This function is part of the range types subdiff function family for timestamp data types
- The result represents the difference in seconds between two timestamps
- PostgreSQL stores timestamps internally as int64 microsecond counts since January 1, 2000
- Division by USECS_PER_SEC converts from internal microsecond representation to user-friendly seconds
- Located in src/backend/utils/adt/rangetypes.c:1664-1674
- Used internally by PostgreSQL's range type system for timestamp range operations requiring time interval calculations