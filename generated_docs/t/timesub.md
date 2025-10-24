# timesub

## Location
[src/timezone/localtime.c:1414-1538](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L1414-L1538)

## Overview
Converts a timestamp to broken-down time fields, accounting for timezone offset and leap seconds.

## Definition

```c
static struct pg_tm *
timesub(const pg_time_t *timep, int32 offset,
		const struct state *sp, struct pg_tm *tmp)
```
## Detailed Description
The  function is a core timezone conversion routine that breaks down a Unix timestamp into calendar components (year, month, day, hour, minute, second, etc.). It handles timezone offsets, leap second corrections, and date calculations across year boundaries. The function performs complex arithmetic to convert seconds since epoch into human-readable date/time components while properly handling leap years and leap seconds.

The function operates in several phases:
1. Applies leap second corrections based on the timezone state
2. Converts the timestamp to days and remaining seconds
3. Iteratively adjusts for year boundaries and leap years
4. Calculates day of week, day of year, and calendar date
5. Breaks down remaining seconds into hours, minutes, and seconds

## Parameters / Member Variables
- `*timep`: Pointer to the timestamp (seconds since Unix epoch) to convert
- `offset`: Timezone offset in seconds to apply to the timestamp
- `*sp`: Pointer to timezone state structure containing leap second and transition information
- `*tmp`: Pointer to pg_tm structure to populate with the broken-down time
## Dependencies
- Functions called/Symbols referenced:
  - [increment_overflow](../i/increment_overflow.md)
  - [leaps_thru_end_of](../l/leaps_thru_end_of.md)
  - isleap
  - EPOCH_YEAR, SECSPERDAY, DAYSPERLYEAR, TM_YEAR_BASE (constants)
  - pg_time_t, pg_tm, lsinfo (types)
- Called from (representative examples):
  - [localsub](../l/localsub.md)
  - [gmtsub](../g/gmtsub.md)

## Notes and Other Information
- Returns NULL and sets errno to EOVERFLOW if the timestamp is out of representable range
- Handles positive leap seconds by setting tm_sec to 60 when a leap second occurs
- Uses careful overflow checking throughout to prevent integer overflow
- The function is static and used internally within the timezone subsystem
- Critical for PostgreSQL's timezone conversion functionality

## Simplified Source

```c
static struct pg_tm *
timesub(const pg_time_t *timep, int32 offset,
        const struct state *sp, struct pg_tm *tmp)
{
    pg_time_t tdays;
    int64 rem;
    int y = EPOCH_YEAR;
    int64 corr = 0;
    bool hit = false;

    // Apply leap second corrections if timezone state available
    if (sp != NULL) {
        for (int i = sp->leapcnt - 1; i >= 0; i--) {
            const struct lsinfo *lp = &sp->lsis[i];
            if (*timep >= lp->ls_trans) {
                corr = lp->ls_corr;
                hit = (*timep == lp->ls_trans &&
                      (i == 0 ? 0 : lp[-1].ls_corr) < corr);
                break;
            }
        }
    }

    // Convert timestamp to days and remaining seconds
    tdays = *timep / SECSPERDAY;
    rem = *timep % SECSPERDAY;

    // Adjust year and days to handle year boundaries
    while (tdays < 0 || tdays >= year_lengths[isleap(y)]) {
        // Calculate year adjustment needed
        int newy = y;
        pg_time_t tdelta = tdays / DAYSPERLYEAR;
        int idelta = (tdelta == 0) ? (tdays < 0 ? -1 : 1) : tdelta;

        if (increment_overflow(&newy, idelta))
            goto out_of_range;

        // Adjust for leap days between years
        int leapdays = leaps_thru_end_of(newy - 1) - leaps_thru_end_of(y - 1);
        tdays -= ((pg_time_t)(newy - y)) * DAYSPERNYEAR + leapdays;
        y = newy;
    }

    int idays = tdays;

    // Apply timezone offset and leap second correction
    rem += offset - corr;

    // Normalize seconds and days
    while (rem < 0) {
        rem += SECSPERDAY;
        --idays;
    }
    while (rem >= SECSPERDAY) {
        rem -= SECSPERDAY;
        ++idays;
    }

    // Normalize days and years
    while (idays < 0) {
        if (increment_overflow(&y, -1))
            goto out_of_range;
        idays += year_lengths[isleap(y)];
    }
    while (idays >= year_lengths[isleap(y)]) {
        idays -= year_lengths[isleap(y)];
        if (increment_overflow(&y, 1))
            goto out_of_range;
    }

    // Set year (adjusted for tm_year base)
    tmp->tm_year = y;
    if (increment_overflow(&tmp->tm_year, -TM_YEAR_BASE))
        goto out_of_range;

    tmp->tm_yday = idays;

    // Calculate day of week
    tmp->tm_wday = (EPOCH_WDAY +
                   ((y - EPOCH_YEAR) % DAYSPERWEEK) * (DAYSPERNYEAR % DAYSPERWEEK) +
                   leaps_thru_end_of(y - 1) - leaps_thru_end_of(EPOCH_YEAR - 1) +
                   idays) % DAYSPERWEEK;
    if (tmp->tm_wday < 0)
        tmp->tm_wday += DAYSPERWEEK;

    // Break down remaining seconds into time components
    tmp->tm_hour = rem / SECSPERHOUR;
    rem %= SECSPERHOUR;
    tmp->tm_min = rem / SECSPERMIN;
    tmp->tm_sec = (rem % SECSPERMIN) + hit;  // Add 1 for leap second

    // Calculate month and day of month
    const int *month_lengths = mon_lengths[isleap(y)];
    for (tmp->tm_mon = 0; idays >= month_lengths[tmp->tm_mon]; tmp->tm_mon++)
        idays -= month_lengths[tmp->tm_mon];
    tmp->tm_mday = idays + 1;

    tmp->tm_isdst = 0;
    tmp->tm_gmtoff = offset;
    return tmp;

out_of_range:
    errno = EOVERFLOW;
    return NULL;
}
```