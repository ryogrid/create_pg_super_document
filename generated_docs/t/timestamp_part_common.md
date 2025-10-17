# timestamp_part_common

## Location
[src/backend/utils/adt/timestamp.c:5353-5610](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5353-L5610)

## Overview
Core implementation function for extracting specific date/time components from timestamp values, supporting both float8 and numeric return types.

## Definition
```c
static Datum timestamp_part_common(PG_FUNCTION_ARGS, bool retnumeric)
```

## Detailed Description
The `timestamp_part_common` function is the central implementation for extracting date/time parts from timestamp values. It serves as the backend for both `timestamp_part()` and `extract_timestamp()` functions. The function handles:

1. **Unit parsing**: Converts string unit names to internal constants
2. **Infinite timestamp handling**: Uses `NonFiniteTimestampTzPart` for infinite values
3. **Finite timestamp processing**: Breaks down timestamps into component parts
4. **Multiple unit types**: Supports time units (seconds, minutes, hours), date units (days, months, years), and special units (epoch, Julian day)
5. **Dual return modes**: Can return either float8 or numeric values based on the `retnumeric` parameter

The function handles complex calculations for derived units like quarters, decades, centuries, millennia, ISO years, and day-of-week/year calculations. It also manages precision issues when dealing with fractional seconds and large timestamp values.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - `units`: Text specifying the unit to extract (e.g., 'year', 'month', 'second')
  - `timestamp`: The timestamp value to extract from
- `retnumeric`: Boolean flag determining return type (true for numeric, false for float8)

## Dependencies
- Functions called/Symbols referenced:
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md) (unit string processing)
  - [DecodeUnits](../D/DecodeUnits.md), DecodeSpecial (unit parsing)
  - [NonFiniteTimestampTzPart](../N/NonFiniteTimestampTzPart.md) (infinite timestamp handling)
  - [timestamp2tm](timestamp2tm.md) (timestamp decomposition)
  - [date2isoweek](../d/date2isoweek.md), date2isoyear (ISO date calculations)
  - [date2j](../d/date2j.md), j2day (Julian day conversions)
  - [SetEpochTimestamp](../S/SetEpochTimestamp.md) (epoch calculations)
  - Various numeric functions (int64_to_numeric, numeric_div_opt_error, etc.)
- Called from (representative examples):
  - [timestamp_part](timestamp_part.md)
  - [extract_timestamp](../e/extract_timestamp.md)

## Notes and Other Information
- Static function serving as common implementation for multiple user-facing functions
- Handles both finite and infinite timestamp values appropriately
- Supports extraction of 20+ different temporal units and components
- Implements complex calendar arithmetic for centuries, decades, and millennia
- Provides high precision numeric results when requested to avoid floating-point precision issues
- Includes extensive error handling for invalid or unsupported units
- Central to PostgreSQL's temporal data extraction functionality across multiple SQL functions

## Simplified Source

```c
static Datum timestamp_part_common(PG_FUNCTION_ARGS, bool retnumeric) {
    text *units = PG_GETARG_TEXT_PP(0);
    Timestamp timestamp = PG_GETARG_TIMESTAMP(1);
    int64 intresult;
    int type, val;
    char *lowunits;
    fsec_t fsec;
    struct pg_tm tt, *tm = &tt;

    // Parse unit string to internal constants
    lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
                                           VARSIZE_ANY_EXHDR(units), false);
    type = DecodeUnits(0, lowunits, &val);
    if (type == UNKNOWN_FIELD)
        type = DecodeSpecial(0, lowunits, &val);

    // Handle infinite timestamps
    if (TIMESTAMP_NOT_FINITE(timestamp)) {
        double r = NonFiniteTimestampTzPart(type, val, lowunits,
                                          TIMESTAMP_IS_NOBEGIN(timestamp), false);
        if (r != 0.0) {
            if (retnumeric) {
                // Return numeric infinity
                if (r < 0)
                    return DirectFunctionCall3(numeric_in,
                                             CStringGetDatum("-Infinity"),
                                             ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(-1));
                else
                    return DirectFunctionCall3(numeric_in,
                                             CStringGetDatum("Infinity"),
                                             ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(-1));
            } else
                PG_RETURN_FLOAT8(r);
        } else
            PG_RETURN_NULL();
    }

    // Process finite timestamps
    if (type == UNITS) {
        // Break down timestamp into components
        if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                           errmsg("timestamp out of range")));

        switch (val) {
            case DTK_MICROSEC:
                intresult = tm->tm_sec * INT64CONST(1000000) + fsec;
                break;
            case DTK_MILLISEC:
                if (retnumeric)
                    PG_RETURN_NUMERIC(int64_div_fast_to_numeric(
                        tm->tm_sec * INT64CONST(1000000) + fsec, 3));
                else
                    PG_RETURN_FLOAT8(tm->tm_sec * 1000.0 + fsec / 1000.0);
                break;
            case DTK_SECOND:
                if (retnumeric)
                    PG_RETURN_NUMERIC(int64_div_fast_to_numeric(
                        tm->tm_sec * INT64CONST(1000000) + fsec, 6));
                else
                    PG_RETURN_FLOAT8(tm->tm_sec + fsec / 1000000.0);
                break;
            case DTK_MINUTE: intresult = tm->tm_min; break;
            case DTK_HOUR: intresult = tm->tm_hour; break;
            case DTK_DAY: intresult = tm->tm_mday; break;
            case DTK_MONTH: intresult = tm->tm_mon; break;
            case DTK_QUARTER: intresult = (tm->tm_mon - 1) / 3 + 1; break;
            case DTK_WEEK:
                intresult = date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);
                break;
            case DTK_YEAR:
                intresult = (tm->tm_year > 0) ? tm->tm_year : tm->tm_year - 1;
                break;
            case DTK_DECADE:
                intresult = (tm->tm_year >= 0) ? tm->tm_year / 10 :
                           -((8 - (tm->tm_year - 1)) / 10);
                break;
            case DTK_CENTURY:
                intresult = (tm->tm_year > 0) ? (tm->tm_year + 99) / 100 :
                           -((99 - (tm->tm_year - 1)) / 100);
                break;
            case DTK_MILLENNIUM:
                intresult = (tm->tm_year > 0) ? (tm->tm_year + 999) / 1000 :
                           -((999 - (tm->tm_year - 1)) / 1000);
                break;
            case DTK_JULIAN:
                // Complex Julian day calculation with fractional day
                if (retnumeric) {
                    // Precise numeric calculation
                    PG_RETURN_NUMERIC(numeric_add_opt_error(
                        int64_to_numeric(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday)),
                        numeric_div_opt_error(
                            int64_to_numeric(((((tm->tm_hour * MINS_PER_HOUR) +
                                              tm->tm_min) * SECS_PER_MINUTE) +
                                            tm->tm_sec) * INT64CONST(1000000) + fsec),
                            int64_to_numeric(SECS_PER_DAY * INT64CONST(1000000)), NULL),
                        NULL));
                } else {
                    PG_RETURN_FLOAT8(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) +
                        ((((tm->tm_hour * MINS_PER_HOUR) + tm->tm_min) * SECS_PER_MINUTE) +
                         tm->tm_sec + (fsec / 1000000.0)) / (double) SECS_PER_DAY);
                }
                break;
            case DTK_ISOYEAR:
                intresult = date2isoyear(tm->tm_year, tm->tm_mon, tm->tm_mday);
                if (intresult <= 0) intresult -= 1; // Adjust BC years
                break;
            case DTK_DOW:
            case DTK_ISODOW:
                intresult = j2day(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday));
                if (val == DTK_ISODOW && intresult == 0) intresult = 7;
                break;
            case DTK_DOY:
                intresult = (date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) -
                            date2j(tm->tm_year, 1, 1) + 1);
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("unit \"%s\" not supported for type %s",
                                     lowunits, format_type_be(TIMESTAMPOID))));
                intresult = 0;
        }
    } else if (type == RESERV) {
        switch (val) {
            case DTK_EPOCH:
                // Calculate seconds since epoch
                Timestamp epoch = SetEpochTimestamp();
                if (retnumeric) {
                    Numeric result;
                    if (timestamp < (PG_INT64_MAX + epoch))
                        result = int64_div_fast_to_numeric(timestamp - epoch, 6);
                    else {
                        result = numeric_div_opt_error(
                            numeric_sub_opt_error(int64_to_numeric(timestamp),
                                                 int64_to_numeric(epoch), NULL),
                            int64_to_numeric(1000000), NULL);
                        result = DatumGetNumeric(DirectFunctionCall2(numeric_round,
                                                NumericGetDatum(result),
                                                Int32GetDatum(6)));
                    }
                    PG_RETURN_NUMERIC(result);
                } else {
                    float8 result = (timestamp < (PG_INT64_MAX + epoch)) ?
                                   (timestamp - epoch) / 1000000.0 :
                                   ((float8) timestamp - epoch) / 1000000.0;
                    PG_RETURN_FLOAT8(result);
                }
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("unit \"%s\" not supported for type %s",
                                     lowunits, format_type_be(TIMESTAMPOID))));
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("unit \"%s\" not recognized for type %s",
                             lowunits, format_type_be(TIMESTAMPOID))));
    }

    // Return final result
    if (retnumeric)
        PG_RETURN_NUMERIC(int64_to_numeric(intresult));
    else
        PG_RETURN_FLOAT8(intresult);
}
```