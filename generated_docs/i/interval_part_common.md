# interval_part_common

## Location
[src/backend/utils/adt/timestamp.c:5951-6142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5951-L6142)

## Overview
Core implementation function for extracting specified fields from PostgreSQL interval values, handling both finite and infinite intervals with optional numeric or float8 return types.

## Definition

```c
struct pg_itm tt,
			   *tm = &tt;
```
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

## Simplified Source

```c
static Datum interval_part_common(PG_FUNCTION_ARGS, bool retnumeric) {
    text *units = PG_GETARG_TEXT_PP(0);
    Interval *interval = PG_GETARG_INTERVAL_P(1);
    int64 intresult;
    int type, val;
    char *lowunits;
    struct pg_itm tt, *tm = &tt;

    // Parse unit string to determine what to extract
    lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
                                            VARSIZE_ANY_EXHDR(units), false);
    type = DecodeUnits(0, lowunits, &val);
    if (type == UNKNOWN_FIELD)
        type = DecodeSpecial(0, lowunits, &val);

    // Handle infinite intervals
    if (INTERVAL_NOT_FINITE(interval)) {
        double r = NonFiniteIntervalPart(type, val, lowunits,
                                         INTERVAL_IS_NOBEGIN(interval));
        if (r != 0.0) {
            // Return infinity in appropriate format
            if (retnumeric) {
                return DirectFunctionCall3(numeric_in,
                    CStringGetDatum(r < 0 ? "-Infinity" : "Infinity"),
                    ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
            } else {
                PG_RETURN_FLOAT8(r);
            }
        } else {
            PG_RETURN_NULL();
        }
    }

    // Handle regular time units
    if (type == UNITS) {
        interval2itm(*interval, tm);
        switch (val) {
            case DTK_MICROSEC:
                intresult = tm->tm_sec * INT64CONST(1000000) + tm->tm_usec;
                break;
            case DTK_MILLISEC:
                if (retnumeric)
                    PG_RETURN_NUMERIC(int64_div_fast_to_numeric(
                        tm->tm_sec * INT64CONST(1000000) + tm->tm_usec, 3));
                else
                    PG_RETURN_FLOAT8(tm->tm_sec * 1000.0 + tm->tm_usec / 1000.0);
                break;
            case DTK_SECOND:
                if (retnumeric)
                    PG_RETURN_NUMERIC(int64_div_fast_to_numeric(
                        tm->tm_sec * INT64CONST(1000000) + tm->tm_usec, 6));
                else
                    PG_RETURN_FLOAT8(tm->tm_sec + tm->tm_usec / 1000000.0);
                break;
            case DTK_MINUTE:
                intresult = tm->tm_min;
                break;
            case DTK_HOUR:
                intresult = tm->tm_hour;
                break;
            case DTK_DAY:
                intresult = tm->tm_mday;
                break;
            case DTK_MONTH:
                intresult = tm->tm_mon;
                break;
            case DTK_QUARTER:
                intresult = (tm->tm_mon / 3) + 1;
                break;
            case DTK_YEAR:
                intresult = tm->tm_year;
                break;
            case DTK_DECADE:
                intresult = tm->tm_year / 10;
                break;
            case DTK_CENTURY:
                intresult = tm->tm_year / 100;
                break;
            case DTK_MILLENNIUM:
                intresult = tm->tm_year / 1000;
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("unit \"%s\" not supported for type %s",
                               lowunits, format_type_be(INTERVALOID))));
        }
    }
    // Handle epoch calculation with overflow protection
    else if (type == RESERV && val == DTK_EPOCH) {
        if (retnumeric) {
            // Calculate seconds from days/months with overflow checking
            int64 secs_from_day_month = ((int64)(4 * DAYS_PER_YEAR) *
                (interval->month / MONTHS_PER_YEAR) +
                (int64)(4 * DAYS_PER_MONTH) * (interval->month % MONTHS_PER_YEAR) +
                (int64)4 * interval->day) * (SECS_PER_DAY / 4);

            int64 val;
            if (!pg_mul_s64_overflow(secs_from_day_month, 1000000, &val) &&
                !pg_add_s64_overflow(val, interval->time, &val)) {
                PG_RETURN_NUMERIC(int64_div_fast_to_numeric(val, 6));
            } else {
                // Fallback to numeric arithmetic for overflow cases
                PG_RETURN_NUMERIC(numeric_add_opt_error(
                    int64_div_fast_to_numeric(interval->time, 6),
                    int64_to_numeric(secs_from_day_month), NULL));
            }
        } else {
            // Float8 calculation is simpler
            float8 result = interval->time / 1000000.0;
            result += ((double)DAYS_PER_YEAR * SECS_PER_DAY) *
                     (interval->month / MONTHS_PER_YEAR);
            result += ((double)DAYS_PER_MONTH * SECS_PER_DAY) *
                     (interval->month % MONTHS_PER_YEAR);
            result += ((double)SECS_PER_DAY) * interval->day;
            PG_RETURN_FLOAT8(result);
        }
    }
    else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("unit \"%s\" not recognized for type %s",
                       lowunits, format_type_be(INTERVALOID))));
    }

    // Return result in requested format
    if (retnumeric)
        PG_RETURN_NUMERIC(int64_to_numeric(intresult));
    else
        PG_RETURN_FLOAT8(intresult);
}
```