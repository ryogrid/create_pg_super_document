# timestamptz_age

## Location
[src/backend/utils/adt/timestamp.c:4393-4546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L4393-L4546)

## Overview
Calculates the time difference between two timestamp with timezone values while retaining year/month fields, producing an interval that preserves calendar semantics rather than absolute time spans.

## Definition
```c
Datum timestamptz_age(PG_FUNCTION_ARGS)
```

## Detailed Description
This function computes the "age" or time difference between two timestamp with timezone (timestamptz) values. It is nearly identical to timestamp_age() but operates on timezone-aware timestamps. Like its counterpart, it preserves year and month components in a way that reflects calendar arithmetic rather than absolute time differences.

The function performs the same key operations as timestamp_age():
1. Handles infinite timestamp values with appropriate error checking
2. Converts timestamptz values to broken-down time structures using timestamp2tm() (which extracts timezone information)
3. Performs field-by-field subtraction of time components
4. Handles negative field propagation (borrowing) across time units  
5. Accounts for variable month lengths when borrowing days
6. Converts the result back to an interval using itm2interval()

A key aspect is that the function deliberately ignores timezone differences between the two input timestamps - it works with the local time components after timezone conversion, making the result independent of timezone offsets.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function argument macro containing:
  - Arg 0: TimestampTz (first timestamp with timezone - "from" time)
  - Arg 1: TimestampTz (second timestamp with timezone - "to" time)

## Dependencies
- Functions called/Symbols referenced:
  - TimestampTz (timestamp with timezone data type)
  - Interval (interval data type)
  - fsec_t (fractional seconds type)
  - [pg_itm](../p/pg_itm.md) (interval time structure)
  - [pg_tm](../p/pg_tm.md) (broken-down time structure)
  - PG_GETARG_TIMESTAMPTZ (PostgreSQL macro)
  - PG_RETURN_INTERVAL_P (PostgreSQL macro)
  - TIMESTAMP_IS_NOBEGIN/TIMESTAMP_IS_NOEND (infinity check macros)
  - INTERVAL_NOBEGIN/INTERVAL_NOEND (infinity interval macros)
  - [timestamp2tm](timestamp2tm.md) (timestamp to broken-down time conversion with timezone)
  - [itm2interval](../i/itm2interval.md) (interval time structure to interval conversion)
  - Time constants: USECS_PER_SEC, SECS_PER_MINUTE, MINS_PER_HOUR, HOURS_PER_DAY, MONTHS_PER_YEAR
  - day_tab (days per month lookup table)
  - isleap (leap year check function)
  - ereport (error reporting function)
  - [palloc](../p/palloc.md) (memory allocation function)
- Called from (representative examples):
  - No direct references found (likely used through PostgreSQL's SQL function infrastructure)

## Notes and Other Information
- This function implements the AGE() SQL function for timestamptz values
- Nearly identical to timestamp_age() but handles timezone-aware timestamps
- Timezone information is extracted during timestamp2tm() conversion but then ignored in the calculation
- The result represents calendar-based time difference, not absolute duration
- Handles infinite timestamps appropriately, treating "infinity - infinity" as an error
- [Complex](../C/Complex.md) borrowing logic ensures proper handling of negative intermediate values
- Month length calculations account for leap years when borrowing days
- Sign handling ensures the result direction matches the timestamp comparison
- Error handling for out-of-range results and invalid timestamp values
- The comment "Note: we deliberately ignore any difference between tz1 and tz2" indicates timezone offsets don't affect the final interval calculation

## Simplified Source

```c
Datum timestamptz_age(PG_FUNCTION_ARGS) {
    TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
    TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
    Interval *result = (Interval *) palloc(sizeof(Interval));

    // Handle infinite timestamps
    if (TIMESTAMP_IS_NOBEGIN(dt1)) {
        if (TIMESTAMP_IS_NOBEGIN(dt2))
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                           errmsg("interval out of range")));
        else
            INTERVAL_NOBEGIN(result);
    } else if (TIMESTAMP_IS_NOEND(dt1)) {
        if (TIMESTAMP_IS_NOEND(dt2))
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                           errmsg("interval out of range")));
        else
            INTERVAL_NOEND(result);
    } else if (TIMESTAMP_IS_NOBEGIN(dt2)) {
        INTERVAL_NOEND(result);
    } else if (TIMESTAMP_IS_NOEND(dt2)) {
        INTERVAL_NOBEGIN(result);
    } else {
        // Convert timestamptz to broken-down time (with timezone info)
        struct pg_itm tm;
        struct pg_tm tm1, tm2;
        fsec_t fsec1, fsec2;
        int tz1, tz2;

        if (timestamp2tm(dt1, &tz1, &tm1, &fsec1, NULL, NULL) == 0 &&
            timestamp2tm(dt2, &tz2, &tm2, &fsec2, NULL, NULL) == 0) {

            // Calculate field-by-field difference
            tm.tm_usec = fsec1 - fsec2;
            tm.tm_sec = tm1.tm_sec - tm2.tm_sec;
            tm.tm_min = tm1.tm_min - tm2.tm_min;
            tm.tm_hour = tm1.tm_hour - tm2.tm_hour;
            tm.tm_mday = tm1.tm_mday - tm2.tm_mday;
            tm.tm_mon = tm1.tm_mon - tm2.tm_mon;
            tm.tm_year = tm1.tm_year - tm2.tm_year;

            // Flip sign if dt1 < dt2
            if (dt1 < dt2) {
                tm.tm_usec = -tm.tm_usec;
                tm.tm_sec = -tm.tm_sec;
                tm.tm_min = -tm.tm_min;
                tm.tm_hour = -tm.tm_hour;
                tm.tm_mday = -tm.tm_mday;
                tm.tm_mon = -tm.tm_mon;
                tm.tm_year = -tm.tm_year;
            }

            // Handle negative field propagation (borrowing)
            while (tm.tm_usec < 0) {
                tm.tm_usec += USECS_PER_SEC;
                tm.tm_sec--;
            }
            while (tm.tm_sec < 0) {
                tm.tm_sec += SECS_PER_MINUTE;
                tm.tm_min--;
            }
            while (tm.tm_min < 0) {
                tm.tm_min += MINS_PER_HOUR;
                tm.tm_hour--;
            }
            while (tm.tm_hour < 0) {
                tm.tm_hour += HOURS_PER_DAY;
                tm.tm_mday--;
            }

            // Handle day borrowing (accounts for variable month lengths)
            while (tm.tm_mday < 0) {
                if (dt1 < dt2) {
                    tm.tm_mday += day_tab[isleap(tm1.tm_year)][tm1.tm_mon - 1];
                    tm.tm_mon--;
                } else {
                    tm.tm_mday += day_tab[isleap(tm2.tm_year)][tm2.tm_mon - 1];
                    tm.tm_mon--;
                }
            }
            while (tm.tm_mon < 0) {
                tm.tm_mon += MONTHS_PER_YEAR;
                tm.tm_year--;
            }

            // Note: timezone differences (tz1 vs tz2) are deliberately ignored

            // Restore sign if necessary
            if (dt1 < dt2) {
                tm.tm_usec = -tm.tm_usec;
                tm.tm_sec = -tm.tm_sec;
                tm.tm_min = -tm.tm_min;
                tm.tm_hour = -tm.tm_hour;
                tm.tm_mday = -tm.tm_mday;
                tm.tm_mon = -tm.tm_mon;
                tm.tm_year = -tm.tm_year;
            }

            if (itm2interval(&tm, result) != 0)
                ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                               errmsg("interval out of range")));
        } else {
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                           errmsg("timestamp out of range")));
        }
    }

    PG_RETURN_INTERVAL_P(result);
}
```