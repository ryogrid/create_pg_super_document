# EncodeTimezone

## Location
[src/backend/utils/adt/datetime.c:4189-4226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L4189-L4226)

## Overview
EncodeTimezone formats a numeric timezone offset into a string representation, handling different formatting styles for date/time output.

## Definition

```c
static char *
EncodeTimezone(char *str, int tz, int style)
```
## Detailed Description
EncodeTimezone converts a timezone offset (given in seconds) into its string representation following standard timezone format conventions. The function handles the conversion of seconds to hours, minutes, and seconds components, and formats them according to the specified style. It correctly handles the sign inversion needed for timezone display (negative offsets are displayed as positive and vice versa). The function supports different formatting styles including XSD date format requirements.

## Parameters / Member Variables
- `*str`: Pointer to the destination string buffer where the timezone representation will be written
- `tz`: Timezone offset in seconds (negative values represent east of UTC, positive west of UTC)
- `style`: Formatting style flag (USE_XSD_DATES forces inclusion of minutes even when zero)
## Dependencies
- Functions called/Symbols referenced:
  - [pg_ultostr_zeropad](../p/pg_ultostr_zeropad.md) (for zero-padded number formatting)
  - SECS_PER_MINUTE (constant for seconds per minute conversion)
  - MINS_PER_HOUR (constant for minutes per hour conversion)
  - USE_XSD_DATES (style constant for XSD date formatting)
- Called from (representative examples):
  - [EncodeTimeOnly](EncodeTimeOnly.md)
  - [EncodeDateTime](EncodeDateTime.md)

## Notes and Other Information
- The function inverts the sign of the timezone offset for display purposes (tz <= 0 becomes '+')
- Returns a pointer to the new end of the string without NUL termination
- Conditionally includes seconds in output only when non-zero
- Conditionally includes minutes based on style or when non-zero
- Uses zero-padded formatting for consistent output width

## Simplified Source

```c
// Simplified version of EncodeTimezone
static char *
EncodeTimezone(char *str, int tz, int style) {
    // Convert seconds to hours, minutes, seconds
    int sec = abs(tz);
    int min = sec / SECS_PER_MINUTE;
    sec -= min * SECS_PER_MINUTE;
    int hour = min / MINS_PER_HOUR;
    min -= hour * MINS_PER_HOUR;

    // Write sign (inverted: negative tz becomes positive display)
    *str++ = (tz <= 0 ? '+' : '-');

    // Format based on what components are non-zero
    if (sec != 0) {
        // Include all components: HH:MM:SS
        str = pg_ultostr_zeropad(str, hour, 2);
        *str++ = ':';
        str = pg_ultostr_zeropad(str, min, 2);
        *str++ = ':';
        str = pg_ultostr_zeropad(str, sec, 2);
    } else if (min != 0 || style == USE_XSD_DATES) {
        // Include hours and minutes: HH:MM
        str = pg_ultostr_zeropad(str, hour, 2);
        *str++ = ':';
        str = pg_ultostr_zeropad(str, min, 2);
    } else {
        // Only hours: HH
        str = pg_ultostr_zeropad(str, hour, 2);
    }

    return str;
}
```

Key simplifications made:
- Added comments explaining the time component conversion logic
- Clarified the sign inversion behavior in comments
- Organized the conditional formatting logic with clear comments for each case
- Preserved the exact logic flow while making the purpose of each branch clearer