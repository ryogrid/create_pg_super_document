# rpytime

## Location
[src/timezone/zic.c:3801-3864](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L3801-L3864)

## Overview
Computes the date (in seconds since January 1, 1970, 00:00 LOCAL time) for a given timezone rule in a specific year.

## Definition
```c
static zic_t rpytime(const struct rule *rp, zic_t wantedy)
```

## Detailed Description
The `rpytime` function calculates the exact timestamp when a timezone rule takes effect in a given year. It handles complex date calculations including leap years, day-of-week dependencies, and special calendar cases like February 29th in non-leap years. The function performs several key operations:

1. Handles boundary cases for minimum and maximum representable times
2. Calculates day offset from epoch year through efficient year cycling
3. Advances through months to reach the target month
4. Handles special February 29th cases in non-leap years
5. Processes day-of-week rules (e.g., "last Sunday", "first Monday >= 8th")
6. Validates that computed dates fall within valid month boundaries
7. Converts final day offset to seconds and adds time-of-day component

## Parameters / Member Variables
- `rp`: Pointer to a timezone rule structure containing month, day, time, and day-code information
- `wantedy`: The target year for which to compute the rule timestamp

## Dependencies
- Functions called/Symbols referenced:
  - `isleap`: Check if year is a leap year
  - `[oadd](../o/oadd.md)`: Overflow-safe addition for day calculations
  - `[tadd](../t/tadd.md)`: Overflow-safe addition for time calculations
  - [error](../e/error.md): Error reporting function
  - [warning](../w/warning.md): Warning message function
- Called from (representative examples):
  - `[inzsub](../i/inzsub.md)`: Timezone initialization subprocess
  - [years_of_observations](../y/years_of_observations.md): Year range calculation function

## Notes and Other Information
- Returns `min_time` or `max_time` for boundary year values (ZIC_MIN/ZIC_MAX)
- Handles negative years and implements efficient year cycling using YEARSPERREPEAT
- Includes special handling for February 29th in non-leap years based on rule type
- Validates day-of-week rules to ensure dates remain within month boundaries
- Uses EPOCH_YEAR (1970) as the reference point for all calculations
- The "nod to Margaret O." comment refers to a humorous variable name for day offset

## Simplified Source

```c
static zic_t
rpytime(const struct rule *rp, zic_t wantedy)
{
    int m, i;
    zic_t dayoff, t, y;

    // Handle boundary cases
    if (wantedy == ZIC_MIN) return min_time;
    if (wantedy == ZIC_MAX) return max_time;

    // Initialize to epoch year and calculate day offset
    dayoff = 0;
    m = TM_JANUARY;
    y = EPOCH_YEAR;

    // Handle year cycling for efficiency with distant years
    if (y < wantedy) {
        wantedy -= y;
        dayoff = (wantedy / YEARSPERREPEAT) * (SECSPERREPEAT / SECSPERDAY);
        wantedy %= YEARSPERREPEAT;
        wantedy += y;
    } else if (wantedy < 0) {
        dayoff = (wantedy / YEARSPERREPEAT) * (SECSPERREPEAT / SECSPERDAY);
        wantedy %= YEARSPERREPEAT;
    }

    // Count days from epoch year to target year
    while (wantedy != y) {
        if (wantedy > y) {
            i = len_years[isleap(y)];
            ++y;
        } else {
            --y;
            i = -len_years[isleap(y)];
        }
        dayoff = oadd(dayoff, i);
    }

    // Add days for months leading up to target month
    while (m != rp->r_month) {
        i = len_months[isleap(y)][m];
        dayoff = oadd(dayoff, i);
        ++m;
    }

    // Handle day of month, with special February 29th logic
    i = rp->r_dayofmonth;
    if (m == TM_FEBRUARY && i == 29 && !isleap(y)) {
        if (rp->r_dycode == DC_DOWLEQ)
            --i;
        else {
            error(_("use of 2/29 in non leap-year"));
            exit(EXIT_FAILURE);
        }
    }
    --i;  // Convert to 0-based
    dayoff = oadd(dayoff, i);

    // Handle day-of-week rules (e.g., "last Sunday", "first Monday >= 8")
    if (rp->r_dycode == DC_DOWGEQ || rp->r_dycode == DC_DOWLEQ) {
        zic_t wday = EPOCH_WDAY;

        // Calculate current day of week
        if (dayoff >= 0)
            wday = (wday + dayoff) % DAYSPERWEEK;
        else {
            wday -= ((-dayoff) % DAYSPERWEEK);
            if (wday < 0) wday += DAYSPERWEEK;
        }

        // Adjust to target day of week
        while (wday != rp->r_wday) {
            if (rp->r_dycode == DC_DOWGEQ) {
                dayoff = oadd(dayoff, 1);
                if (++wday >= DAYSPERWEEK) wday = 0;
                ++i;
            } else {
                dayoff = oadd(dayoff, -1);
                if (--wday < 0) wday = DAYSPERWEEK - 1;
                --i;
            }
        }

        // Warn if rule goes past month boundaries
        if (i < 0 || i >= len_months[isleap(y)][m]) {
            if (noise)
                warning(_("rule goes past start/end of month"));
        }
    }

    // Convert days to seconds and add time-of-day
    if (dayoff < min_time / SECSPERDAY) return min_time;
    if (dayoff > max_time / SECSPERDAY) return max_time;
    t = (zic_t) dayoff * SECSPERDAY;
    return tadd(t, rp->r_tod);
}
```