# timestamptz_part_common

## Location
[src/backend/utils/adt/timestamp.c:5626-5882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5626-L5882)

## Overview
The core implementation function that extracts specified date/time fields from timestamp with time zone values, handling timezone conversion and supporting both floating-point and numeric return types.

## Definition
```c
static Datum timestamptz_part_common(PG_FUNCTION_ARGS, bool retnumeric)
```

## Detailed Description
The `timestamptz_part_common` function is the shared implementation for extracting date/time components from timestamp with time zone (timestamptz) values. Unlike its timestamp counterpart, this function must handle timezone conversions and timezone-specific fields like timezone offset hours/minutes. The function supports extracting a wide variety of temporal components including standard date/time parts (year, month, day, hour, minute, second), timezone components (timezone offset), ISO standards (ISO year, ISO week, ISO day of week), and special values (Julian day, epoch seconds).

The function handles both finite and infinite timestamps, with special logic for infinite values. It supports two return types controlled by the `retnumeric` parameter: floating-point numbers (float8) for compatibility and precision numeric types for higher accuracy, especially important for fractional seconds and epoch calculations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention
  - Argument 0 (`units`): Text specifying the field to extract (e.g., 'year', 'month', 'timezone', etc.)
  - Argument 1 (`timestamp`): TimestampTz value to extract from
- `retnumeric`: Boolean flag controlling return type (true for numeric, false for float8)

## Dependencies
- Functions called/Symbols referenced:
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md) - Normalizes field name input
  - [DecodeUnits](../D/DecodeUnits.md), `DecodeSpecial` - Parse field name tokens  
  - [NonFiniteTimestampTzPart](../N/NonFiniteTimestampTzPart.md) - Handles infinite timestamp values
  - [timestamp2tm](timestamp2tm.md) - Converts timestamp to broken-down time structure
  - [date2isoweek](../d/date2isoweek.md), `date2isoyear`, `date2j`, `j2day` - Date calculation utilities
  - [int64_div_fast_to_numeric](../i/int64_div_fast_to_numeric.md), `numeric_add_opt_error` - [Numeric](../N/Numeric.md) type operations
  - [SetEpochTimestamp](../S/SetEpochTimestamp.md) - Gets PostgreSQL epoch reference point
  - Various PostgreSQL datum conversion macros
- Called from (representative examples):
  - [timestamptz_part](timestamptz_part.md) - Float8 variant of field extraction
  - [extract_timestamptz](../e/extract_timestamptz.md) - [Numeric](../N/Numeric.md) variant of field extraction

## Notes and Other Information
- Handles timezone-specific fields: DTK_TZ (timezone offset in seconds), DTK_TZ_MINUTE, DTK_TZ_HOUR
- Supports both standard and ISO date/time standards (ISO year, ISO week, ISO day of week)  
- Special handling for BCE years in decade/century/millennium calculations
- Precision handling for fractional seconds using microsecond internal representation
- Julian day calculations support both integer and fractional parts for precise astronomical use
- Comprehensive error handling for unsupported or unrecognized field names
- Performance optimization for epoch calculations to avoid precision loss with large timestamps
- Located in `src/backend/utils/adt/timestamp.c:5626-5882`
- Static function - only accessible within the timestamp.c compilation unit

## Simplified Source

```c
static Datum timestamptz_part_common(PG_FUNCTION_ARGS, bool retnumeric) {
    text *units = PG_GETARG_TEXT_PP(0);
    TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
    int64 intresult;
    int tz, type, val;
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
                                          TIMESTAMP_IS_NOBEGIN(timestamp), true);
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

    // Process finite timestamps with timezone conversion
    if (type == UNITS) {
        // Break down timestamptz into components with timezone
        if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, NULL) != 0)
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                           errmsg("timestamp out of range")));

        switch (val) {
            // Timezone-specific fields
            case DTK_TZ: intresult = -tz; break;
            case DTK_TZ_MINUTE: intresult = (-tz / SECS_PER_MINUTE) % MINS_PER_HOUR; break;
            case DTK_TZ_HOUR: intresult = -tz / SECS_PER_HOUR; break;

            // Standard time components (same as timestamp)
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

            // Date calculations
            case DTK_WEEK:
                intresult = date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);
                break;
            case DTK_YEAR:
                intresult = (tm->tm_year > 0) ? tm->tm_year : tm->tm_year - 1;
                break;
            case DTK_DECADE:
                intresult = (tm->tm_year > 0) ? tm->tm_year / 10 :
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

            // Special calculations
            case DTK_JULIAN:
                if (retnumeric) {
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
                if (intresult <= 0) intresult -= 1;
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
                                     lowunits, format_type_be(TIMESTAMPTZOID))));
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
                                     lowunits, format_type_be(TIMESTAMPTZOID))));
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("unit \"%s\" not recognized for type %s",
                             lowunits, format_type_be(TIMESTAMPTZOID))));
    }

    // Return final result
    if (retnumeric)
        PG_RETURN_NUMERIC(int64_to_numeric(intresult));
    else
        PG_RETURN_FLOAT8(intresult);
}
```