# tstzrange_subdiff

## Location
[src/backend/utils/adt/rangetypes.c:1675-1702](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1675-L1702)

## Overview
Computes the difference between two timestamp with time zone values for use in timestamptz range type operations, returning the result as a float8 value representing the number of seconds.

## Definition
Datum tstzrange_subdiff(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the subdiff function for tstzrange (timestamp with time zone range) types in PostgreSQL. It takes two timestamp with time zone values and computes their difference in seconds, returning the result as a float8 value. Despite being for timestamptz types, the internal storage and calculation are identical to regular timestamps since PostgreSQL stores timestamptz values internally as UTC timestamps (microseconds since the PostgreSQL epoch).

The function performs arithmetic subtraction on the timestamp values (stored as int64 microsecond counts) and then divides by USECS_PER_SEC (1000000) to convert from microseconds to seconds. Time zone information is handled at higher levels in PostgreSQL, so this subdiff function operates on the underlying UTC timestamp values directly.

## Parameters / Member Variables
- v1: First timestamptz value (Timestamp/int64) - microseconds since PostgreSQL epoch in UTC, the minuend in the subtraction
- v2: Second timestamptz value (Timestamp/int64) - microseconds since PostgreSQL epoch in UTC, the subtrahend in the subtraction
- result: Calculated difference in seconds (float8)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP (macro for extracting Timestamp arguments)
  - USECS_PER_SEC (constant defining microseconds per second, value 1000000)  
  - PG_RETURN_FLOAT8 (macro for returning float8 values)
- Type definitions:
  - Timestamp (PostgreSQL timestamp type, used for both timestamp and timestamptz storage)

## Notes and Other Information
- This function is part of the range types subdiff function family for timestamptz data types
- Implementation is identical to tsrange_subdiff since both operate on UTC microsecond values internally  
- The result represents the difference in seconds between two timestamptz values
- Time zone handling occurs at higher levels; this function works with underlying UTC timestamps
- PostgreSQL stores timestamptz internally as int64 microsecond counts since January 1, 2000 UTC
- Located in src/backend/utils/adt/rangetypes.c:1675-1683
- Used internally by PostgreSQL's range type system for timestamptz range operations requiring time interval calculations

## Simplified Source

```c
Datum tstzrange_subdiff(PG_FUNCTION_ARGS) {
    // Extract the two timestamptz arguments (microseconds since epoch in UTC)
    Timestamp v1 = PG_GETARG_TIMESTAMP(0);
    Timestamp v2 = PG_GETARG_TIMESTAMP(1);

    // Calculate difference in seconds by converting from microseconds
    float8 result = ((float8) v1 - (float8) v2) / USECS_PER_SEC;

    PG_RETURN_FLOAT8(result);
}
```