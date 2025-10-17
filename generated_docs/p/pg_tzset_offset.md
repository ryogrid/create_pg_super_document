# pg_tzset_offset

## Location
[src/timezone/pgtz.c:320-360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/pgtz.c#L320-L360)

## Overview
Creates a timezone object for a fixed GMT offset specified in seconds, used primarily for SQL-spec SET TIME ZONE INTERVAL operations.

## Definition
```c
pg_tz *pg_tzset_offset(long gmtoffset)
```

## Detailed Description
This function creates a timezone object representing a fixed offset from GMT. It converts the numeric offset into a POSIX timezone string format and then uses pg_tzset() to load the timezone. The function handles the conversion from seconds to a properly formatted timezone string with hours, minutes, and seconds components as needed.

The function uses POSIX sign convention for the input (positive values meaning west of Greenwich) but creates a timezone string that follows ISO sign convention in the displayable abbreviation. This dual convention handling ensures compatibility with both POSIX timezone specifications and user expectations.

## Parameters / Member Variables
- `gmtoffset`: The GMT offset in seconds using POSIX sign convention (positive values = west of Greenwich, negative = east)

## Dependencies
- Functions called/Symbols referenced:
  - SECS_PER_HOUR (constant for seconds per hour conversion)
  - SECS_PER_MINUTE (constant for seconds per minute conversion)  
  - [pg_tzset](pg_tzset.md) (loads timezone from POSIX timezone string)
- Called from (representative examples):
  - [check_timezone](../c/check_timezone.md) (in variable.c for timezone validation)
  - [DecodeTimezoneNameToTz](../D/DecodeTimezoneNameToTz.md) (in datetime.c for timezone parsing)

## Notes and Other Information
- Can return NULL if the specified offset is outside the range allowed by the zic library
- Uses snprintf for safe string formatting to avoid buffer overflows
- Constructs timezone names in format like '<-05>+05' for +5 hour offset or '<+03>-03' for -3 hour offset
- The timezone string format follows POSIX TZ environment variable conventions
- Used specifically for SQL interval-based timezone specifications

## Simplified Source

```c
pg_tz *pg_tzset_offset(long gmtoffset)
{
    long absoffset = (gmtoffset < 0) ? -gmtoffset : gmtoffset;
    char offsetstr[64];
    char tzname[128];

    // Format hours
    snprintf(offsetstr, sizeof(offsetstr), "%02ld", absoffset / SECS_PER_HOUR);
    absoffset %= SECS_PER_HOUR;

    // Add minutes if non-zero
    if (absoffset != 0) {
        snprintf(offsetstr + strlen(offsetstr), sizeof(offsetstr) - strlen(offsetstr),
                 ":%02ld", absoffset / SECS_PER_MINUTE);
        absoffset %= SECS_PER_MINUTE;

        // Add seconds if non-zero
        if (absoffset != 0)
            snprintf(offsetstr + strlen(offsetstr), sizeof(offsetstr) - strlen(offsetstr),
                     ":%02ld", absoffset);
    }

    // Create POSIX timezone string with ISO sign convention in display name
    if (gmtoffset > 0)
        snprintf(tzname, sizeof(tzname), "<-%s>+%s", offsetstr, offsetstr);
    else
        snprintf(tzname, sizeof(tzname), "<+%s>-%s", offsetstr, offsetstr);

    return pg_tzset(tzname);
}
```