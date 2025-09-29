# transtime

## Location
[src/timezone/localtime.c:839-935](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L839-L935)

## Overview
Calculates the exact year-relative time (in seconds from start of year) when a timezone rule takes effect, given a specific year, rule, and UTC offset.

## Definition
```c
static int32 transtime(const int year, const struct rule *const rulep, const int32 offset)
```

## Detailed Description
The transtime function converts abstract timezone rules into concrete transition times for a given year. It handles three different rule types and calculates when daylight saving time transitions occur:

**For Julian Day (Jn) rules:**
- Converts Julian day numbers (1-365) to seconds from start of year
- Handles leap years by adding an extra day for days >= 60 (March 1 and later)
- Julian days exclude leap day from counting

**For Day of Year (n) rules:**
- Simply multiplies day number by seconds per day
- Includes leap day in the counting (0-365)

**For Month/Week/Day (Mm.w.d) rules:**
- Uses Zeller's Congruence algorithm to determine the day of week for the first day of the specified month
- Calculates which specific date corresponds to "nth occurrence of weekday d in month m"
- Handles "week 5" as "last occurrence" by checking month boundaries
- Accumulates days from previous months to get the absolute day of year

The final result includes the rule's specified time offset and the current UTC offset to produce the exact transition moment.

## Parameters / Member Variables
- `year`: The year for which to calculate the transition time
- `rulep`: Pointer to the rule structure containing the transition specification
- `offset`: Current UTC offset in seconds at the time the rule takes effect

## Dependencies
- Functions called/Symbols referenced:
  - isleap (to determine if the year is a leap year)
  - INITIALIZE (macro for variable initialization)
  - JULIAN_DAY, DAY_OF_YEAR, MONTH_NTH_DAY_OF_WEEK (rule type constants)
  - SECSPERDAY, DAYSPERWEEK (time conversion constants)
  - mon_lengths (array containing days in each month for leap/non-leap years)
- Called from (representative examples):
  - [tzparse](tzparse.md)

## Notes and Other Information
- This is a static function used internally within the timezone parsing subsystem
- The function implements complex date calculations required for POSIX timezone rules
- Zeller's Congruence is used for accurate day-of-week calculations across different calendar systems
- The algorithm correctly handles leap years and month boundary conditions
- Returns time in seconds from the beginning of the specified year (year-relative time)
- The final result accounts for both the rule's specified transition time and current UTC offset
- Critical for accurate daylight saving time transitions in different timezone configurations

## Simplified Source

```c
// Simplified version of transtime
static int32
transtime(const int year, const struct rule *const rulep, const int32 offset)
{
    bool leapyear = isleap(year);
    int32 value;

    switch (rulep->r_type) {
        case JULIAN_DAY:
            // Julian day: 1-365, leap day handling for days >= 60
            value = (rulep->r_day - 1) * SECSPERDAY;
            if (leapyear && rulep->r_day >= 60)
                value += SECSPERDAY;  // Add leap day
            break;

        case DAY_OF_YEAR:
            // Simple day of year: just multiply by seconds per day
            value = rulep->r_day * SECSPERDAY;
            break;

        case MONTH_NTH_DAY_OF_WEEK:
            // Find nth occurrence of weekday in month
            // Use Zeller's Congruence for first day of month
            int m1 = (rulep->r_mon + 9) % 12 + 1;
            int yy0 = (rulep->r_mon <= 2) ? (year - 1) : year;
            int yy1 = yy0 / 100;
            int yy2 = yy0 % 100;
            int dow = ((26 * m1 - 2) / 10 + 1 + yy2 + yy2 / 4 + yy1 / 4 - 2 * yy1) % 7;
            if (dow < 0) dow += DAYSPERWEEK;

            // Find the target day occurrence
            int d = rulep->r_day - dow;
            if (d < 0) d += DAYSPERWEEK;

            // Advance to nth week, checking month boundaries
            for (int i = 1; i < rulep->r_week; ++i) {
                if (d + DAYSPERWEEK >= mon_lengths[leapyear][rulep->r_mon - 1])
                    break;
                d += DAYSPERWEEK;
            }

            // Convert to seconds and add previous months
            value = d * SECSPERDAY;
            for (int i = 0; i < rulep->r_mon - 1; ++i)
                value += mon_lengths[leapyear][i] * SECSPERDAY;
            break;
    }

    // Add rule time and current UTC offset
    return value + rulep->r_time + offset;
}
```

Key simplifications made:
- Removed detailed variable declarations and consolidated them inline
- Simplified variable naming (removed cryptic single-letter names where possible)
- Added clear comments explaining each rule type's purpose
- Consolidated the Zeller's Congruence calculation into a more readable flow
- Removed INITIALIZE macro usage for clarity
- Maintained the essential algorithm structure while improving readability
- Preserved all critical logic including leap year handling and month boundary checks