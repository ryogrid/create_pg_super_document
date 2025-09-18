# daterange_subdiff

## Location
[src/backend/utils/adt/rangetypes.c:1655-1663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1655-L1663)

## Overview
Computes the difference between two date values for use in date range type operations, returning the result as a float8 value representing the number of days.

## Definition
Datum daterange_subdiff(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the subdiff function for daterange types in PostgreSQL. It takes two date values (internally represented as int32 day counts since the PostgreSQL epoch) and computes their difference, converting the result to a float8 value. The resulting float8 represents the number of days between the two dates.

The function performs simple arithmetic subtraction since PostgreSQL dates are stored internally as the number of days since January 1, 2000 (the PostgreSQL epoch). The difference directly gives the number of days between the dates, which is then cast to float8 for consistency with other range subdiff functions.

## Parameters / Member Variables
- v1: First date value (int32) - represented as days since PostgreSQL epoch, the minuend in the subtraction
- v2: Second date value (int32) - represented as days since PostgreSQL epoch, the subtrahend in the subtraction

## Dependencies  
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro for extracting int32 arguments)
  - PG_RETURN_FLOAT8 (macro for returning float8 values)

## Notes and Other Information
- This function is part of the range types subdiff function family for date data types
- The result represents the difference in days between two dates
- PostgreSQL stores dates internally as int32 day counts since January 1, 2000
- Simple implementation similar to int4range_subdiff since dates use int32 storage
- Located in src/backend/utils/adt/rangetypes.c:1655-1663
- Used internally by PostgreSQL's range type system for date range operations requiring difference calculations