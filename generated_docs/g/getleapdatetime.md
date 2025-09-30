# getleapdatetime

## Location
[src/timezone/zic.c:1666-1754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1666-L1754)

## Overview
Parses and validates leap second date/time fields and converts them to a zic_t timestamp value for leap second processing.

## Definition

```c
struct lookup *lp;
```
## Detailed Description
The  function processes leap second date and time information from input fields and converts them to an internal timestamp representation (zic_t). The function performs comprehensive validation and calculation:

- Parses and validates the year field
- Tracks leap year ranges (leapminyear/leapmaxyear) for non-expiration lines
- Calculates day offset from EPOCH_YEAR by iterating through years and accounting for leap years
- Validates and converts month name to numeric value using mon_names lookup table
- Adds days for each month from January to the target month
- Validates day of month against calendar limits for the given year/month
- Converts the final day offset to seconds and adds time-of-day
- Performs bounds checking against min_time/max_time limits
- Ensures the final timestamp is not before the Unix epoch

## Parameters / Member Variables
- : Array of string pointers containing leap second date/time fields
- : Number of fields provided (unused in function body)
- : Boolean indicating if this is an expiration line (affects leap year tracking)

## Dependencies
- Functions called/Symbols referenced:
  - sscanf (parsing numeric fields)
  - [error](../e/error.md) (error reporting)
  - isleap (leap year checking)
  - [oadd](../o/oadd.md) (overflow-safe addition)
  - [byword](../b/byword.md) (lookup table search)
  - [gethms](gethms.md) (time parsing)
  - [tadd](../t/tadd.md) (time addition)
- Called from (representative examples):
  - [inleap](../i/inleap.md) (processes leap second lines)
  - [inexpires](../i/inexpires.md) (processes leap expiration lines)

## Notes and Other Information
- Returns -1 on validation errors, otherwise returns computed zic_t timestamp
- Uses field indices LP_YEAR, LP_MONTH, LP_DAY, LP_TIME to access date/time components
- Maintains global variables leapseen, leapminyear, leapmaxyear for leap second tracking
- Uses len_years and len_months arrays for calendar calculations
- Performs extensive bounds checking and validation
- Part of PostgreSQL's timezone data compilation system (zic) for processing leap second data
- The comment 'Leapin' Lizards!' is a playful reference to leap years/seconds

## Simplified Source

```c
static zic_t getleapdatetime(char **fields, int nfields, bool expire_line) {
    int year, month, day;
    zic_t dayoff = 0, tod, t;
    char xs;
    const struct lookup *lp;

    // Parse and validate year
    if (sscanf(fields[LP_YEAR], "%d%c", &year, &xs) != 1) {
        error(_("invalid leaping year"));
        return -1;
    }

    // Track leap year ranges for non-expiration lines
    if (!expire_line) {
        if (!leapseen || leapmaxyear < year)
            leapmaxyear = year;
        if (!leapseen || leapminyear > year)
            leapminyear = year;
        leapseen = true;
    }

    // Calculate days from EPOCH_YEAR to target year
    zic_t j = EPOCH_YEAR;
    while (j != year) {
        if (year > j) {
            dayoff = oadd(dayoff, len_years[isleap(j)]);
            j++;
        } else {
            j--;
            dayoff = oadd(dayoff, -len_years[isleap(j)]);
        }
    }

    // Parse and validate month
    if ((lp = byword(fields[LP_MONTH], mon_names)) == NULL) {
        error(_("invalid month name"));
        return -1;
    }
    month = lp->l_value;

    // Add days for months from January to target month
    for (j = TM_JANUARY; j != month; j++) {
        dayoff = oadd(dayoff, len_months[isleap(year)][j]);
    }

    // Parse and validate day of month
    if (sscanf(fields[LP_DAY], "%d%c", &day, &xs) != 1 ||
        day <= 0 || day > len_months[isleap(year)][month]) {
        error(_("invalid day of month"));
        return -1;
    }

    dayoff = oadd(dayoff, day - 1);

    // Check time bounds
    if (dayoff < min_time / SECSPERDAY || dayoff > max_time / SECSPERDAY) {
        error(_("time too small or too large"));
        return -1;
    }

    // Convert to seconds and add time of day
    t = dayoff * SECSPERDAY;
    tod = gethms(fields[LP_TIME], _("invalid time of day"));
    t = tadd(t, tod);

    if (t < 0)
        error(_("leap second precedes Epoch"));

    return t;
}
```