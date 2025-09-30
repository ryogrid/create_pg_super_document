# pg_next_dst_boundary

## Location
[src/timezone/localtime.c:1610-1756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L1610-L1756)

## Overview
Finds the next daylight saving time (DST) transition boundary after a given timestamp within a specified timezone.

## Definition

```c
struct state *sp;
```
## Detailed Description
The `pg_next_dst_boundary` function locates the next DST transition point after a specified timestamp within a given timezone. It returns detailed information about the timezone state both before and after the transition, including GMT offsets and DST status.

The function handles several scenarios:
1. **DST-less zones**: Returns 0 with current timezone information
2. **Extrapolation**: For timestamps outside the transition table, it extrapolates using repeating patterns
3. **Binary search**: For timestamps within the transition table, it uses binary search to efficiently locate the next boundary
4. **Edge cases**: Handles times before the first transition or after the last known transition

The function is critical for PostgreSQL's timezone handling, particularly for operations that need to account for DST transitions when performing date/time arithmetic.

## Parameters / Member Variables
- `timep`: Pointer to the timestamp to find the next DST boundary after
- `before_gmtoff`: Output parameter for GMT offset before the boundary
- `before_isdst`: Output parameter for DST status before the boundary
- `boundary`: Output parameter for the timestamp of the DST boundary
- `after_gmtoff`: Output parameter for GMT offset after the boundary
- `after_isdst`: Output parameter for DST status after the boundary
- `tz`: Timezone structure containing transition information

## Dependencies
- Functions called/Symbols referenced:
  - [pg_next_dst_boundary](pg_next_dst_boundary.md) (recursive call for extrapolation)
  - pg_time_t, pg_tz, ttinfo (timezone-related types)
  - YEARSPERREPEAT, AVGSECSPERYEAR (constants for extrapolation)
- Called from (representative examples):
  - [DetermineTimeZoneOffsetInternal](../D/DetermineTimeZoneOffsetInternal.md) (for timezone offset calculations)

## Notes and Other Information
- Returns 1 if a DST boundary is found, 0 if no boundary exists after the given time, -1 on failure
- Uses binary search for efficient lookup within transition tables
- Supports extrapolation for timestamps outside known transition data using repeating patterns
- Handles both forward and backward extrapolation for historical and future dates
- Essential for accurate timezone calculations in PostgreSQL's datetime functionality
- The function is part of PostgreSQL's public timezone API

## Simplified Source

```c
int pg_next_dst_boundary(const pg_time_t *timep,
                        long int *before_gmtoff,
                        int *before_isdst,
                        pg_time_t *boundary,
                        long int *after_gmtoff,
                        int *after_isdst,
                        const pg_tz *tz) {
    const struct state *sp = &tz->state;
    const pg_time_t t = *timep;

    // Handle DST-less zones
    if (sp->timecnt == 0) {
        // Find first non-DST type
        int i = 0;
        while (sp->ttis[i].tt_isdst && ++i < sp->typecnt);
        if (i >= sp->typecnt) i = 0;

        const struct ttinfo *ttisp = &sp->ttis[i];
        *before_gmtoff = ttisp->tt_utoff;
        *before_isdst = ttisp->tt_isdst;
        return 0;  // No DST transitions
    }

    // Handle extrapolation for times outside transition table
    if ((sp->goback && t < sp->ats[0]) || (sp->goahead && t > sp->ats[sp->timecnt - 1])) {
        // Calculate offset for extrapolation pattern
        pg_time_t newt = t;
        pg_time_t seconds = (t < sp->ats[0]) ?
                           sp->ats[0] - t : t - sp->ats[sp->timecnt - 1];

        // Apply repeating cycle calculation
        pg_time_t cycles = (seconds / YEARSPERREPEAT / AVGSECSPERYEAR) + 1;
        seconds = cycles * YEARSPERREPEAT * AVGSECSPERYEAR;

        if (t < sp->ats[0])
            newt += seconds;
        else
            newt -= seconds;

        // Recursive call with adjusted time
        int result = pg_next_dst_boundary(&newt, before_gmtoff, before_isdst,
                                        boundary, after_gmtoff, after_isdst, tz);
        if (result == 1) {
            if (t < sp->ats[0])
                *boundary -= seconds;
            else
                *boundary += seconds;
        }
        return result;
    }

    // Handle time at or past last transition
    if (t >= sp->ats[sp->timecnt - 1]) {
        const struct ttinfo *ttisp = &sp->ttis[sp->types[sp->timecnt - 1]];
        *before_gmtoff = ttisp->tt_utoff;
        *before_isdst = ttisp->tt_isdst;
        return 0;  // No more transitions
    }

    // Handle time before first transition
    if (t < sp->ats[0]) {
        // Use standard time for "before"
        int i = 0;
        while (sp->ttis[i].tt_isdst && ++i < sp->typecnt);
        const struct ttinfo *ttisp = &sp->ttis[i];
        *before_gmtoff = ttisp->tt_utoff;
        *before_isdst = ttisp->tt_isdst;

        // First transition is the boundary
        *boundary = sp->ats[0];
        ttisp = &sp->ttis[sp->types[0]];
        *after_gmtoff = ttisp->tt_utoff;
        *after_isdst = ttisp->tt_isdst;
        return 1;
    }

    // Binary search for next transition
    int lo = 1, hi = sp->timecnt - 1;
    while (lo < hi) {
        int mid = (lo + hi) >> 1;
        if (t < sp->ats[mid])
            hi = mid;
        else
            lo = mid + 1;
    }

    // Set up before/after states
    const struct ttinfo *before_ttinfo = &sp->ttis[sp->types[lo - 1]];
    *before_gmtoff = before_ttinfo->tt_utoff;
    *before_isdst = before_ttinfo->tt_isdst;

    *boundary = sp->ats[lo];

    const struct ttinfo *after_ttinfo = &sp->ttis[sp->types[lo]];
    *after_gmtoff = after_ttinfo->tt_utoff;
    *after_isdst = after_ttinfo->tt_isdst;

    return 1;
}
```