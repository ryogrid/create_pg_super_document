# timestamptz_trunc_internal

## Location
[src/backend/utils/adt/timestamp.c:4826-4969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L4826-L4969)

## Overview
Internal implementation function that provides timezone-aware timestamp truncation functionality, shared by both `timestamptz_trunc` and `timestamptz_trunc_zone`.

## Definition
```c
static TimestampTz timestamptz_trunc_internal(text *units, TimestampTz timestamp, pg_tz *tzp)
```

## Detailed Description
The `timestamptz_trunc_internal` function is the core implementation for timezone-aware timestamp truncation operations. Unlike the plain `timestamp_trunc` function, this version properly handles timezone conversions during the truncation process.

Key differences from plain timestamp truncation:
1. Accepts a timezone parameter (`pg_tz *tzp`) for timezone-aware operations
2. Uses `timestamp2tm` with timezone information to break down the timestamp
3. Sets `redotz = true` for truncations at day level and above, indicating timezone offset recalculation is needed
4. Calls `DetermineTimeZoneOffset` when `redotz` is true to handle potential DST transitions
5. Uses `tm2timestamp` with timezone information to reconstruct the final timestamp

This function is essential for ensuring that timestamp truncation behaves correctly across different timezones and handles edge cases like daylight saving time transitions appropriately.

## Parameters / Member Variables
- `units` (text*): The time unit to truncate to (e.g., 'year', 'month', 'day', 'hour', etc.)
- `timestamp` (TimestampTz): The timezone-aware timestamp to be truncated  
- `tzp` (pg_tz*): The timezone context for the truncation operation

## Dependencies
- Functions called/Symbols referenced:
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md)
  - [DecodeUnits](../D/DecodeUnits.md)
  - [timestamp2tm](timestamp2tm.md)
  - [date2isoweek](../d/date2isoweek.md)
  - [isoweek2date](../i/isoweek2date.md)
  - [DetermineTimeZoneOffset](../D/DetermineTimeZoneOffset.md)
  - [tm2timestamp](tm2timestamp.md)
  - [format_type_be](../f/format_type_be.md)
  - Various DTK_* constants (DTK_WEEK, DTK_YEAR, DTK_MONTH, etc.)
  - MONTHS_PER_YEAR
- Called from (representative examples):
  - [timestamptz_trunc](timestamptz_trunc.md)
  - [timestamptz_trunc_zone](timestamptz_trunc_zone.md)

## Notes and Other Information
- This is an internal static function that consolidates the timezone-aware truncation logic
- The `redotz` flag determines when timezone offset recalculation is necessary (for truncations at day level and above)
- Properly handles timezone transitions that may occur when truncating to day boundaries
- Uses the same time unit constants and validation logic as the non-timezone version
- Includes special handling for weeks using ISO week calculations
- Millennium/century/decade calculations use the same boundary logic as plain timestamp truncation
- The function assumes that infinite timestamps have already been handled by the caller
- Critical for maintaining timezone correctness during truncation operations

## Simplified Source

```c
static TimestampTz timestamptz_trunc_internal(text *units, TimestampTz timestamp, pg_tz *tzp) {
    TimestampTz result;
    int tz, type, val;
    bool redotz = false;
    char *lowunits;
    fsec_t fsec;
    struct pg_tm tm;

    // Parse unit string (case-insensitive)
    lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
                                           VARSIZE_ANY_EXHDR(units), false);
    type = DecodeUnits(0, lowunits, &val);

    if (type == UNITS) {
        // Convert timestamptz to broken-down time with timezone info
        if (timestamp2tm(timestamp, &tz, &tm, &fsec, NULL, tzp) != 0)
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
                    redotz = true;
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
                redotz = true;  // Timezone recalculation needed for day+ truncations
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
                                      lowunits, format_type_be(TIMESTAMPTZOID))));
                result = 0;
        }

        // Recalculate timezone offset if needed (for DST transitions)
        if (redotz)
            tz = DetermineTimeZoneOffset(&tm, tzp);

        // Convert back to timestamptz
        if (tm2timestamp(&tm, fsec, &tz, &result) != 0)
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                           errmsg("timestamp out of range")));
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("unit \"%s\" not recognized for type %s",
                              lowunits, format_type_be(TIMESTAMPTZOID))));
        result = 0;
    }

    return result;
}
```