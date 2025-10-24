# stringrule

## Location
[src/timezone/zic.c:2716-2796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L2716-L2796)

## Overview
Converts a timezone rule to its string representation in POSIX timezone format, handling both day-of-month and day-of-week rule types with time offset calculations.

## Definition

```c
static int
stringrule(char *result, struct rule *const rp, zic_t save, zic_t stdoff)
```
## Detailed Description
The  function generates a POSIX timezone rule string from a PostgreSQL timezone rule structure. It handles two main types of daylight saving time transition rules:

1. **Day-of-month rules (DC_DOM)**: Fixed calendar dates like "March 15th"
2. **Day-of-week rules (DC_DOWGEQ/DC_DOWLEQ)**: Relative dates like "first Sunday in March" or "last Sunday in October"

The function formats the rule into POSIX timezone format (e.g., "M3.2.0" for second Sunday in March) and applies time offset corrections based on whether the rule time is in UTC, standard time, or wall clock time. It returns a compatibility year indicating the earliest POSIX version that supports the generated rule format.

## Parameters / Member Variables
- `*result`: Output buffer where the formatted rule string will be written
- `rp`: Pointer to the rule structure containing the transition rule definition
- `save`: The daylight saving time offset to apply
- `stdoff`: The standard time offset from UTC
## Dependencies
- Functions called/Symbols referenced:
  -  (standard library function for string formatting)
  -  (formats time offset into string)
  -  (array containing days per month)
- Called from (representative examples):
  -  (generates complete timezone string representations)

## Notes and Other Information
- Returns -1 for unsupported rule types (like February 29th in non-leap years)
- Returns a compatibility year (0, 1994, or 2013) indicating POSIX version requirements
- Handles leap year considerations by rejecting February 29th rules
- Optimizes output by omitting the 'J' prefix for January and February dates
- Applies complex time offset calculations for UTC, standard time, and wall clock time conversions
- Part of PostgreSQL's timezone compilation system (zic) for generating binary timezone files

## Simplified Source

```c
static int stringrule(char *result, struct rule *const rp, zic_t save, zic_t stdoff) {
    zic_t transition_time = rp->r_tod;
    int compatibility_year = 0;

    if (rp->r_dycode == DC_DOM) {
        // Day-of-month rule (e.g., "March 15th")

        // Reject Feb 29 to avoid leap year issues
        if (rp->r_dayofmonth == 29 && rp->r_month == TM_FEBRUARY) {
            return -1;
        }

        // Calculate day-of-year
        int day_of_year = 0;
        for (int month = 0; month < rp->r_month; month++) {
            day_of_year += len_months[0][month];
        }

        // Format as either "N" or "JN" (omit J for Jan/Feb)
        if (rp->r_month <= 1) {
            result += sprintf(result, "%d", day_of_year + rp->r_dayofmonth - 1);
        } else {
            result += sprintf(result, "J%d", day_of_year + rp->r_dayofmonth);
        }
    } else {
        // Day-of-week rule (e.g., "first Sunday in March")

        int week, weekday = rp->r_wday, weekday_offset;

        if (rp->r_dycode == DC_DOWGEQ) {
            // "First X on or after day N"
            weekday_offset = (rp->r_dayofmonth - 1) % DAYSPERWEEK;
            if (weekday_offset) compatibility_year = 2013;
            weekday -= weekday_offset;
            transition_time += weekday_offset * SECSPERDAY;
            week = 1 + (rp->r_dayofmonth - 1) / DAYSPERWEEK;
        } else if (rp->r_dycode == DC_DOWLEQ) {
            // "Last X on or before day N"
            if (rp->r_dayofmonth == len_months[1][rp->r_month]) {
                week = 5;  // Last week of month
            } else {
                weekday_offset = rp->r_dayofmonth % DAYSPERWEEK;
                if (weekday_offset) compatibility_year = 2013;
                weekday -= weekday_offset;
                transition_time += weekday_offset * SECSPERDAY;
                week = rp->r_dayofmonth / DAYSPERWEEK;
            }
        } else {
            return -1;  // Unsupported rule type
        }

        // Normalize weekday
        if (weekday < 0) weekday += DAYSPERWEEK;

        // Format as "M<month>.<week>.<weekday>"
        result += sprintf(result, "M%d.%d.%d", rp->r_month + 1, week, weekday);
    }

    // Apply time offset corrections based on time type
    if (rp->r_todisut) transition_time += stdoff;      // UTC time
    if (rp->r_todisstd && !rp->r_isdst) transition_time += save;  // Standard time

    // Add time specification if not default (2:00 AM)
    if (transition_time != 2 * SECSPERMIN * MINSPERHOUR) {
        *result++ = '/';
        if (!stringoffset(result, transition_time)) {
            return -1;
        }

        // Update compatibility year based on time value
        if (transition_time < 0) {
            if (compatibility_year < 2013) compatibility_year = 2013;
        } else if (transition_time >= SECSPERDAY) {
            if (compatibility_year < 1994) compatibility_year = 1994;
        }
    }

    return compatibility_year;
}
```

**Key simplifications:**
- Added descriptive variable names (`compatibility_year`, `transition_time`, etc.)
- Grouped related logic with clear comments explaining each rule type
- Explained the complex offset calculations and their purposes
- Clarified the POSIX format being generated (M<month>.<week>.<weekday>)
- Preserved the essential timezone rule conversion logic