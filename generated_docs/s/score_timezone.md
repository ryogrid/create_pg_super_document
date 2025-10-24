# score_timezone

## Location
[src/bin/initdb/findtimezone.c:234-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/findtimezone.c#L234-L319)

## Overview
Evaluates how well a specific timezone setting matches the system's timezone behavior by testing it against a series of reference time points.

## Definition
```c
static int score_timezone(const char *tzname, struct tztry *tt)
```

## Detailed Description
This function assesses the compatibility of a given timezone with the system's local timezone behavior by comparing PostgreSQL's timezone calculations against the system's localtime() results for a series of test timestamps. It loads the specified timezone definition and tests it against multiple time points, returning a score that represents the number of successful matches. The function performs comprehensive validation including leap second checks, time component comparisons, and timezone abbreviation matching. A higher score indicates better compatibility, with -1 indicating complete incompatibility.

## Parameters / Member Variables
- `tzname`: Name of the timezone to evaluate (e.g., "America/New_York")
- `tt`: Pointer to a tztry structure containing test timestamps and related data for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [pg_load_tz](../p/pg_load_tz.md)
  - [pg_tz_acceptable](../p/pg_tz_acceptable.md)  
  - [pg_localtime](../p/pg_localtime.md)
  - [compare_tm](../c/compare_tm.md)
  - localtime
  - strftime
  - strcmp
- Types referenced:
  - tztry
  - pg_time_t
  - [pg_tm](../p/pg_tm.md)
  - [pg_tz](../p/pg_tz.md)
- Called from (representative examples):
  - [perfect_timezone_match](../p/perfect_timezone_match.md)
  - [scan_available_timezones](scan_available_timezones.md)

## Notes and Other Information
- Returns -1 for completely unusable timezone settings (unrecognized name, uses leap seconds, etc.)
- Returns 0+ indicating the number of test times that matched successfully
- Test times are processed in order until the first mismatch is found
- Includes debug output when DEBUG_IDENTIFY_TIMEZONE is defined
- Validates both time values and timezone abbreviations for comprehensive matching
- Part of initdb's timezone detection mechanism to find the best system timezone match

## Simplified Source

```c
static int score_timezone(const char *tzname, struct tztry *tt)
{
    int i;
    pg_time_t pgtt;
    struct tm *systm;
    struct pg_tm *pgtm;
    char cbuf[TZ_STRLEN_MAX + 1];
    pg_tz *tz;

    // Load and validate timezone definition
    tz = pg_load_tz(tzname);
    if (!tz)
        return -1;  // Unrecognized timezone name

    // Reject timezones that use leap seconds
    if (!pg_tz_acceptable(tz))
        return -1;

    // Test timezone against all reference times
    for (i = 0; i < tt->n_test_times; i++)
    {
        pgtt = (pg_time_t)(tt->test_times[i]);

        // Get PostgreSQL's time conversion
        pgtm = pg_localtime(&pgtt, tz);
        if (!pgtm)
            return -1;

        // Get system's time conversion
        systm = localtime(&(tt->test_times[i]));
        if (!systm)
            return i;  // System has no data for this time

        // Compare time components (year, month, day, hour, min, sec, dst)
        if (!compare_tm(systm, pgtm))
            return i;  // Time mismatch found

        // Compare timezone abbreviations if available
        if (systm->tm_isdst >= 0)
        {
            if (pgtm->tm_zone == NULL)
                return -1;

            strftime(cbuf, sizeof(cbuf) - 1, "%Z", systm);
            if (strcmp(cbuf, pgtm->tm_zone) != 0)
                return i;  // Zone abbreviation mismatch
        }
    }

    return i;  // All test times matched successfully
}
```