# tm2timestamp

## Location
[src/backend/utils/adt/timestamp.c:1997-2046](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1997-L2046)

## Overview
Converts a POSIX time structure (struct pg_tm) to a PostgreSQL timestamp data type, performing validation and overflow checking.

## Definition

```c
int
tm2timestamp(struct pg_tm *tm, fsec_t fsec, int *tzp, Timestamp *result)
```
## Detailed Description
The  function converts human-readable time components stored in a  structure into PostgreSQL's internal timestamp representation. The conversion process includes:

1. **Input Validation**: Checks if the date components form a valid Julian date to prevent overflow
2. **Date Conversion**: Converts Gregorian date (year/month/day) to Julian day number, then adjusts to PostgreSQL's J2000 epoch
3. **Time Assembly**: Combines time components (hour/minute/second/fractional seconds) into microseconds since midnight
4. **Timestamp Construction**: Combines date and time into a single timestamp value (days * USECS_PER_DAY + time)
5. **Overflow Protection**: Multiple overflow checks ensure the result fits within timestamp range
6. **Timezone Adjustment**: Optionally applies timezone offset using dt2local()
7. **Final Validation**: Ensures the result is within valid timestamp bounds

## Parameters / Member Variables
- `*tm`: Input struct pg_tm containing time components (year, month, day, hour, minute, second)
- `fsec`: Fractional seconds component in microseconds
- `*tzp`: Timezone offset in seconds to apply, or NULL for no timezone conversion
- `*result`: Output parameter to store the resulting timestamp value
## Dependencies
- Functions called/Symbols referenced:
  - IS_VALID_JULIAN (macro to validate Julian date bounds)
  - [date2j](../d/date2j.md) (Gregorian to Julian day conversion)
  - [time2t](time2t.md) (time components to microseconds conversion)
  - [dt2local](../d/dt2local.md) (timezone adjustment)
  - IS_VALID_TIMESTAMP (macro to validate timestamp range)
  - POSTGRES_EPOCH_JDATE, USECS_PER_DAY (epoch and time constants)
- Called from (representative examples):
  - [timestamp_in](timestamp_in.md) (string to timestamp parsing)
  - [timestamptz_in](timestamptz_in.md) (string to timestamptz parsing)
  - [timestamp_pl_interval](timestamp_pl_interval.md) (timestamp arithmetic)
  - [to_timestamp](to_timestamp.md) (formatting function)
  - [timestamp_trunc](timestamp_trunc.md) (date/time truncation)

## Notes and Other Information
- Returns 0 on success, -1 on failure (out of range or overflow)
- Year values are full years (not 1900-based like standard C tm structure)
- Month values are 1-based (1-12), not 0-based like standard C
- Includes special case handling for 24:00:00 (midnight of next day)
- Performs multiple overflow checks to prevent integer overflow during calculation
- The function is the inverse of timestamp2tm and is essential for timestamp input/parsing operations
- All intermediate calculations use TimeOffset type to handle potential overflow scenarios

## Simplified Source

```c
int
tm2timestamp(struct pg_tm *tm, fsec_t fsec, int *tzp, Timestamp *result)
{
    // Validate input date
    if (!IS_VALID_JULIAN(tm->tm_year, tm->tm_mon, tm->tm_mday)) {
        *result = 0;
        return -1;
    }

    // Convert date to days since PostgreSQL epoch (J2000)
    TimeOffset date = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - POSTGRES_EPOCH_JDATE;

    // Convert time to microseconds since midnight
    TimeOffset time = time2t(tm->tm_hour, tm->tm_min, tm->tm_sec, fsec);

    // Combine date and time
    *result = date * USECS_PER_DAY + time;

    // Check for overflow
    if ((*result - time) / USECS_PER_DAY != date) {
        *result = 0;
        return -1;
    }

    // Check for range errors (just-barely overflow cases)
    if ((*result < 0 && date > 0) || (*result > 0 && date < -1)) {
        *result = 0;
        return -1;
    }

    // Apply timezone offset if requested
    if (tzp != NULL) {
        *result = dt2local(*result, -(*tzp));
    }

    // Final validation
    if (!IS_VALID_TIMESTAMP(*result)) {
        *result = 0;
        return -1;
    }

    return 0;
}
```