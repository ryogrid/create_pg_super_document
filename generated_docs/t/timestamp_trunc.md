# timestamp_trunc

## Location
[src/backend/utils/adt/timestamp.c:4618-4751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L4618-L4751)

## Overview
Truncates a timestamp to a specified time unit, effectively rounding down to the beginning of the specified time period.

## Definition
```c
Datum timestamp_trunc(PG_FUNCTION_ARGS)
```

## Detailed Description
The `timestamp_trunc` function truncates a timestamp value to a specified time unit (such as 'year', 'month', 'day', 'hour', etc.). This is commonly used for time-series analysis to group timestamps into regular intervals. The function works by:

1. Converting the timestamp to a broken-down time structure
2. Based on the specified unit, zeroing out all smaller time components
3. For larger units (decade, century, millennium), calculating the appropriate boundary
4. Converting the modified time structure back to a timestamp

The function supports a wide range of time units from microseconds up to millennia, with special handling for weeks (using ISO week calculations), quarters, and larger calendar periods.

## Parameters / Member Variables
- `units` (text*): The time unit to truncate to (e.g., 'year', 'month', 'day', 'hour', 'minute', 'second', 'millisecond', 'microsecond', 'week', 'quarter', 'decade', 'century', 'millennium')
- `timestamp` (Timestamp): The timestamp value to be truncated

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - PG_GETARG_TIMESTAMP
  - TIMESTAMP_NOT_FINITE
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md)
  - [DecodeUnits](../D/DecodeUnits.md)
  - [timestamp2tm](timestamp2tm.md)
  - [tm2timestamp](tm2timestamp.md)
  - [date2isoweek](../d/date2isoweek.md)
  - [isoweek2date](../i/isoweek2date.md)
  - [format_type_be](../f/format_type_be.md)
  - PG_RETURN_TIMESTAMP
  - Various DTK_* constants (DTK_WEEK, DTK_YEAR, DTK_MONTH, etc.)
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- Supports truncation from microseconds to millennia with proper handling of calendar boundaries
- Week truncation uses ISO week calculations, which may result in dates from the previous or next year
- For negative years, millennium/century/decade calculations use special logic to handle BC dates correctly  
- Quarter truncation rounds down to the beginning of the quarter (Jan, Apr, Jul, Oct)
- The function preserves infinite timestamp values without modification
- Includes comprehensive error handling for invalid units and out-of-range values
- Uses a case-insensitive unit string matching system

## Simplified Source

```c
Datum timestamp_trunc(PG_FUNCTION_ARGS) {
    text *units = PG_GETARG_TEXT_PP(0);
    Timestamp timestamp = PG_GETARG_TIMESTAMP(1);
    Timestamp result;
    int type, val;
    char *lowunits;
    fsec_t fsec;
    struct pg_tm tm;

    // Return infinite timestamps as-is
    if (TIMESTAMP_NOT_FINITE(timestamp))
        PG_RETURN_TIMESTAMP(timestamp);

    // Parse unit string (case-insensitive)
    lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
                                           VARSIZE_ANY_EXHDR(units), false);
    type = DecodeUnits(0, lowunits, &val);

    if (type == UNITS) {
        // Convert timestamp to broken-down time
        if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL) != 0)
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                           errmsg("timestamp out of range")));

        // Truncate based on specified unit
        switch (val) {
            case DTK_WEEK:
                {
                    int woy = date2isoweek(tm.tm_year, tm.tm_mon, tm.tm_mday);

                    // Handle year boundary weeks
                    if (woy >= 52 && tm.tm_mon == 1)
                        --tm.tm_year;
                    if (woy <= 1 && tm.tm_mon == MONTHS_PER_YEAR)
                        ++tm.tm_year;

                    isoweek2date(woy, &(tm.tm_year), &(tm.tm_mon), &(tm.tm_mday));
                    tm.tm_hour = 0;
                    tm.tm_min = 0;
                    tm.tm_sec = 0;
                    fsec = 0;
                    break;
                }
            case DTK_MILLENNIUM:
                // Round to millennium boundaries: -1000, 1, 1001, 2001...
                if (tm.tm_year > 0)
                    tm.tm_year = ((tm.tm_year + 999) / 1000) * 1000 - 999;
                else
                    tm.tm_year = -((999 - (tm.tm_year - 1)) / 1000) * 1000 + 1;
                /* FALL THRU */
            case DTK_CENTURY:
                // Round to century boundaries: -100, 1, 101...
                if (tm.tm_year > 0)
                    tm.tm_year = ((tm.tm_year + 99) / 100) * 100 - 99;
                else
                    tm.tm_year = -((99 - (tm.tm_year - 1)) / 100) * 100 + 1;
                /* FALL THRU */
            case DTK_DECADE:
                // Round to decade boundaries (only if not already processed)
                if (val != DTK_MILLENNIUM && val != DTK_CENTURY) {
                    if (tm.tm_year > 0)
                        tm.tm_year = (tm.tm_year / 10) * 10;
                    else
                        tm.tm_year = -((8 - (tm.tm_year - 1)) / 10) * 10;
                }
                /* FALL THRU */
            case DTK_YEAR:
                tm.tm_mon = 1;
                /* FALL THRU */
            case DTK_QUARTER:
                tm.tm_mon = (3 * ((tm.tm_mon - 1) / 3)) + 1;
                /* FALL THRU */
            case DTK_MONTH:
                tm.tm_mday = 1;
                /* FALL THRU */
            case DTK_DAY:
                tm.tm_hour = 0;
                /* FALL THRU */
            case DTK_HOUR:
                tm.tm_min = 0;
                /* FALL THRU */
            case DTK_MINUTE:
                tm.tm_sec = 0;
                /* FALL THRU */
            case DTK_SECOND:
                fsec = 0;
                break;
            case DTK_MILLISEC:
                fsec = (fsec / 1000) * 1000;
                break;
            case DTK_MICROSEC:
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("unit \"%s\" not supported for type %s",
                                      lowunits, format_type_be(TIMESTAMPOID))));
                result = 0;
        }

        // Convert back to timestamp
        if (tm2timestamp(&tm, fsec, NULL, &result) != 0)
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                           errmsg("timestamp out of range")));
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("unit \"%s\" not recognized for type %s",
                              lowunits, format_type_be(TIMESTAMPOID))));
        result = 0;
    }

    PG_RETURN_TIMESTAMP(result);
}
```