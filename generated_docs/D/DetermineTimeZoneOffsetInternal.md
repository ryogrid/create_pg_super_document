# DetermineTimeZoneOffsetInternal

## Location
[src/backend/utils/adt/datetime.c:1607-1745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L1607-L1745)

## Overview
The core implementation for timezone offset calculation that handles DST transitions, ambiguous times, and edge cases while also returning the computed UTC time value.

## Definition
```c
static int DetermineTimeZoneOffsetInternal(struct pg_tm *tm, pg_tz *tzp, pg_time_t *tp)
```

## Detailed Description
DetermineTimeZoneOffsetInternal performs the complex work of determining timezone offsets while handling all the edge cases that arise with DST transitions. It converts the given local date/time to a UTC timestamp and determines the appropriate GMT offset, carefully handling situations where local times are either invalid (during spring-forward transitions) or ambiguous (during fall-back transitions).

The function uses pg_next_dst_boundary to find DST transition points and implements sophisticated logic to resolve ambiguous times. For invalid times during spring-forward, it prefers the "before" interpretation; for ambiguous times during fall-back, it prefers the "after" interpretation. This approach provides consistent behavior regardless of whether standard or daylight time is considered "normal" for the zone.

The implementation avoids using the system's mktime() function for performance and reliability reasons, instead performing direct calculations using PostgreSQL's internal time representation.

## Parameters / Member Variables
- `tm`: Pointer to pg_tm struct with input date/time fields, will have tm_isdst set on output
- `tzp`: Pointer to timezone definition containing DST rules and offset information
- `tp`: Output parameter that receives the calculated UTC time as pg_time_t

## Dependencies
- Functions called/Symbols referenced:
  - IS_VALID_JULIAN (macro to validate Julian date range)
  - [date2j](../d/date2j.md) (convert date to Julian day number)
  - [pg_next_dst_boundary](../p/pg_next_dst_boundary.md) (find next DST transition boundary)
  - UNIX_EPOCH_JDATE, SECS_PER_DAY, MINS_PER_HOUR, SECS_PER_MINUTE (time constants)
- Called from (representative examples):
  - [DetermineTimeZoneOffset](DetermineTimeZoneOffset.md) (public wrapper function)
  - [DetermineTimeZoneAbbrevOffset](DetermineTimeZoneAbbrevOffset.md) (timezone abbreviation handling)

## Notes and Other Information
- Returns GMT offset in seconds (negative of the timezone's UTC offset)
- Sets tp to 0 and returns 0 for out-of-range dates instead of throwing errors
- Assumes all GMT offsets are less than 24 hours and DST boundaries are at least 48 hours apart
- Handles overflow detection for pg_time_t arithmetic operations
- Implements consistent disambiguation rules for invalid/ambiguous times during DST transitions
- Significantly faster than system mktime() implementations
- Static function not exposed globally to prevent misuse of overflow behavior

## Simplified Source

```c
static int
DetermineTimeZoneOffsetInternal(struct pg_tm *tm, pg_tz *tzp, pg_time_t *tp)
{
    int date, sec;
    pg_time_t day, mytime, prevtime, boundary, beforetime, aftertime;
    long int before_gmtoff, after_gmtoff;
    int before_isdst, after_isdst;
    int res;

    // Convert date/time to GMT timestamp
    if (!IS_VALID_JULIAN(tm->tm_year, tm->tm_mon, tm->tm_mday))
        goto overflow;

    date = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - UNIX_EPOCH_JDATE;
    day = ((pg_time_t)date) * SECS_PER_DAY;
    if (day / SECS_PER_DAY != date)
        goto overflow;

    sec = tm->tm_sec + (tm->tm_min + tm->tm_hour * MINS_PER_HOUR) * SECS_PER_MINUTE;
    mytime = day + sec;
    if (mytime < 0 && day > 0)
        goto overflow;

    // Find DST boundary around this time
    prevtime = mytime - SECS_PER_DAY;
    if (mytime < 0 && prevtime > 0)
        goto overflow;

    res = pg_next_dst_boundary(&prevtime, &before_gmtoff, &before_isdst,
                               &boundary, &after_gmtoff, &after_isdst, tzp);
    if (res < 0)
        goto overflow;

    // Simple case: no DST transitions
    if (res == 0) {
        tm->tm_isdst = before_isdst;
        *tp = mytime - before_gmtoff;
        return -(int)before_gmtoff;
    }

    // Handle DST transitions and ambiguous times
    beforetime = mytime - before_gmtoff;
    aftertime = mytime - after_gmtoff;

    // Check for overflow in offset calculations
    if ((before_gmtoff > 0 && mytime < 0 && beforetime > 0) ||
        (before_gmtoff <= 0 && mytime > 0 && beforetime < 0) ||
        (after_gmtoff > 0 && mytime < 0 && aftertime > 0) ||
        (after_gmtoff <= 0 && mytime > 0 && aftertime < 0))
        goto overflow;

    // Determine which side of boundary we're on
    if (beforetime < boundary && aftertime < boundary) {
        tm->tm_isdst = before_isdst;
        *tp = beforetime;
        return -(int)before_gmtoff;
    }
    if (beforetime > boundary && aftertime >= boundary) {
        tm->tm_isdst = after_isdst;
        *tp = aftertime;
        return -(int)after_gmtoff;
    }

    // Handle invalid/ambiguous times during transitions
    if (beforetime > aftertime) {
        // Spring forward: prefer "before" interpretation
        tm->tm_isdst = before_isdst;
        *tp = beforetime;
        return -(int)before_gmtoff;
    }
    // Fall back: prefer "after" interpretation
    tm->tm_isdst = after_isdst;
    *tp = aftertime;
    return -(int)after_gmtoff;

overflow:
    // Out of range: assume UTC
    tm->tm_isdst = 0;
    *tp = 0;
    return 0;
}
```