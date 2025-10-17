# make_timestamp_internal

## Location
[src/backend/utils/adt/timestamp.c:572-653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L572-L653)

## Overview
Internal workhorse function that constructs a Timestamp value from individual date and time components, performing comprehensive validation and overflow checks.

## Definition

```c
struct pg_tm tm;
```
## Detailed Description
The  function is the core implementation for creating PostgreSQL Timestamp values from separate date and time components. This static function serves as the foundation for both  and  functions.

The function performs extensive validation of input parameters, including:
- Date field validation using 
- Julian date range checking with 
- Time overflow detection via 
- Multiple levels of timestamp overflow checking

It handles negative years by treating them as BC (Before Christ) dates and converts the final result to PostgreSQL's internal timestamp representation based on microseconds since the PostgreSQL epoch.

## Parameters
- : The year component (negative values are treated as BC dates)
- : The month component (1-12)
- : The day component (1-31, depending on month)
- : The hour component (0-23)
- : The minute component (0-59)
- : The second component (0-59.999999, allows fractional seconds)

## Dependencies
- Functions called/Symbols referenced:
  - [ValidateDate](../V/ValidateDate.md)
  - IS_VALID_JULIAN
  - [date2j](../d/date2j.md)
  - [float_time_overflows](../f/float_time_overflows.md)
  - IS_VALID_TIMESTAMP
  - Constants: DTK_DATE_M, POSTGRES_EPOCH_JDATE, MINS_PER_HOUR, SECS_PER_MINUTE, USECS_PER_SEC, USECS_PER_DAY
- Called from:
  - [make_timestamp](make_timestamp.md)
  - [make_timestamptz](make_timestamptz.md)
  - [make_timestamptz_at_timezone](make_timestamptz_at_timezone.md)

## Notes and Other Information
- This is a static function, not directly accessible outside of timestamp.c
- Performs multiple overflow checks at different stages to ensure timestamp validity
- Supports the special case of '1999-12-31 24:00:00' as a valid timestamp
- Uses microsecond precision for internal timestamp representation
- Throws ERROR with specific error codes for various validation failures (ERRCODE_DATETIME_FIELD_OVERFLOW, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)

## Simplified Source

```c
static Timestamp make_timestamp_internal(int year, int month, int day,
                                        int hour, int min, double sec) {
    struct pg_tm tm;
    TimeOffset date, time;
    bool bc = false;

    // Set up time structure
    tm.tm_year = year;
    tm.tm_mon = month;
    tm.tm_mday = day;

    // Handle negative years as BC dates
    if (tm.tm_year < 0) {
        bc = true;
        tm.tm_year = -tm.tm_year;
    }

    // Validate date components
    if (ValidateDate(DTK_DATE_M, false, false, bc, &tm) != 0) {
        ereport(ERROR, "date field value out of range");
    }

    // Check Julian date range
    if (!IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday)) {
        ereport(ERROR, "date out of range");
    }

    // Convert to days since PostgreSQL epoch
    date = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

    // Validate time components
    if (float_time_overflows(hour, min, sec)) {
        ereport(ERROR, "time field value out of range");
    }

    // Convert time to microseconds
    time = (((hour * MINS_PER_HOUR + min) * SECS_PER_MINUTE) * USECS_PER_SEC)
           + (int64) rint(sec * USECS_PER_SEC);

    // Combine date and time
    Timestamp result = date * USECS_PER_DAY + time;

    // Final range checks
    if ((result - time) / USECS_PER_DAY != date || !IS_VALID_TIMESTAMP(result)) {
        ereport(ERROR, "timestamp out of range");
    }

    return result;
}
```