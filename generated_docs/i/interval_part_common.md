# interval_part_common

## Location
[src/backend/utils/adt/timestamp.c:5951-6142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5951-L6142)

## Overview
Core implementation function for extracting specified fields from PostgreSQL interval values, handling both finite and infinite intervals with optional numeric or float8 return types.

## Definition


## Detailed Description
This static function serves as the common implementation for both interval_part() and extract_interval() functions. It parses the requested time unit from a text input, handles special cases for infinite intervals by delegating to NonFiniteIntervalPart(), and performs field extraction for finite intervals.

The function supports two return modes: numeric (exact decimal) or float8 (floating point), controlled by the retnumeric parameter. For finite intervals, it converts the interval to an internal time structure (pg_itm) and extracts the requested component. Special handling is provided for fractional seconds (milliseconds, seconds) and epoch calculations.

For infinite intervals, the function calls NonFiniteIntervalPart to determine whether to return infinity, negative infinity, or NULL based on the unit type and interval direction.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - units: Text string specifying the time unit to extract
  - interval: The interval value to extract from
- : Boolean flag determining return type (true = numeric, false = float8)

## Dependencies
- Functions called/Symbols referenced:
  - [NonFiniteIntervalPart](../N/NonFiniteIntervalPart.md) (for infinite interval handling)
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md) (unit name processing)
  - [DecodeUnits](../D/DecodeUnits.md), DecodeSpecial (unit parsing)
  - [interval2itm](interval2itm.md) (interval to time structure conversion)
  - [int64_div_fast_to_numeric](int64_div_fast_to_numeric.md), int64_to_numeric (numeric conversions)
  - [numeric_add_opt_error](../n/numeric_add_opt_error.md) (numeric arithmetic)
  - DirectFunctionCall3 (for numeric infinity values)
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md), pg_add_s64_overflow (overflow checking)
- Constants referenced:
  - DTK_* constants (time unit identifiers)
  - UNITS, RESERV, UNKNOWN_FIELD (unit type categories)
  - Time conversion constants (SECS_PER_DAY, DAYS_PER_MONTH, etc.)
- Macros used:
  - INTERVAL_NOT_FINITE, INTERVAL_IS_NOBEGIN (infinite interval checks)
  - PG_GETARG_TEXT_PP, PG_GETARG_INTERVAL_P (argument extraction)
  - PG_RETURN_NUMERIC, PG_RETURN_FLOAT8, PG_RETURN_NULL (return values)
- Called from:
  - [interval_part](interval_part.md)
  - [extract_interval](../e/extract_interval.md)

## Notes and Other Information
The function includes careful overflow handling for epoch calculations, falling back to numeric arithmetic when int64 operations would overflow. Division operations for decade, century, and millennium extraction include comments about potential negative remainders in C division. The implementation prioritizes accuracy for numeric return types while maintaining performance for float8 operations.