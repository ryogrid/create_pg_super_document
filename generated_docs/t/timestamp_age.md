# timestamp_age

## Location
[src/backend/utils/adt/timestamp.c:4247-4392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L4247-L4392)

## Overview
Calculates the time difference between two timestamps while retaining year/month fields, producing an interval that preserves calendar semantics rather than absolute time spans.

## Definition
```c
Datum timestamp_age(PG_FUNCTION_ARGS)
```

## Detailed Description
This function computes the "age" or time difference between two timestamp values. Unlike simple timestamp subtraction, this function preserves year and month components in a way that reflects calendar arithmetic rather than absolute time differences. This means that the result accounts for variable month lengths and leap years.

The function performs several key operations:
1. Handles infinite timestamp values with appropriate error checking
2. Converts timestamps to broken-down time structures using timestamp2tm()
3. Performs field-by-field subtraction of time components
4. Handles negative field propagation (borrowing) across time units
5. Accounts for variable month lengths when borrowing days
6. Converts the result back to an interval using itm2interval()

The calculation is complex because it maintains calendar semantics - for example, the difference between Jan 31 and Mar 1 should be reported as 1 month and X days, accounting for February's actual length.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function argument macro containing:
  - Arg 0: Timestamp (first timestamp - "from" time)
  - Arg 1: Timestamp (second timestamp - "to" time)

## Dependencies
- Functions called/Symbols referenced:
  - Timestamp (timestamp data type)
  - Interval (interval data type)  
  - fsec_t (fractional seconds type)
  - [pg_itm](../p/pg_itm.md) (interval time structure)
  - [pg_tm](../p/pg_tm.md) (broken-down time structure)
  - PG_GETARG_TIMESTAMP (PostgreSQL macro)
  - PG_RETURN_INTERVAL_P (PostgreSQL macro)
  - TIMESTAMP_IS_NOBEGIN/TIMESTAMP_IS_NOEND (infinity check macros)
  - INTERVAL_NOBEGIN/INTERVAL_NOEND (infinity interval macros)
  - [timestamp2tm](timestamp2tm.md) (timestamp to broken-down time conversion)
  - [itm2interval](../i/itm2interval.md) (interval time structure to interval conversion)
  - Time constants: USECS_PER_SEC, SECS_PER_MINUTE, MINS_PER_HOUR, HOURS_PER_DAY, MONTHS_PER_YEAR
  - day_tab (days per month lookup table)
  - isleap (leap year check function)
  - ereport (error reporting function)
  - [palloc](../p/palloc.md) (memory allocation function)
- Called from (representative examples):
  - No direct references found (likely used through PostgreSQL's SQL function infrastructure)

## Notes and Other Information
- This function implements the AGE() SQL function for timestamp values
- The result is not an accurate absolute time span due to calendar arithmetic - year and month components lose absolute meaning once computed
- Handles infinite timestamps appropriately, treating "infinity - infinity" as an error
- The complex borrowing logic ensures proper handling of negative intermediate values
- Month length calculations account for leap years when borrowing days
- Sign handling ensures the result direction matches the timestamp comparison
- Error handling for out-of-range results and invalid timestamp values
- Related to timestamptz_age but operates on timestamp without timezone values

## Simplified Source

```c
Datum timestamp_age(PG_FUNCTION_ARGS) {
    Timestamp dt1 = PG_GETARG_TIMESTAMP(0);
    Timestamp dt2 = PG_GETARG_TIMESTAMP(1);
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
        // Convert timestamps to broken-down time
        struct pg_itm tm;
        struct pg_tm tm1, tm2;
        fsec_t fsec1, fsec2;

        if (timestamp2tm(dt1, NULL, &tm1, &fsec1, NULL, NULL) == 0 &&
            timestamp2tm(dt2, NULL, &tm2, &fsec2, NULL, NULL) == 0) {

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
            // Propagate from microseconds up to years
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