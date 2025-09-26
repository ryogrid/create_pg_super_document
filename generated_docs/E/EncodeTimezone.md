# EncodeTimezone

## Location
src/backend/utils/adt/datetime.c: 4189 - 4226

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
- : Pointer to the destination string buffer where the timezone representation will be written
- : Timezone offset in seconds (negative values represent east of UTC, positive west of UTC)  
- : Formatting style flag (USE_XSD_DATES forces inclusion of minutes even when zero)

## Dependencies
- Functions called/Symbols referenced:
  - pg_ultostr_zeropad (for zero-padded number formatting)
  - SECS_PER_MINUTE (constant for seconds per minute conversion)
  - MINS_PER_HOUR (constant for minutes per hour conversion)
  - USE_XSD_DATES (style constant for XSD date formatting)
- Called from (representative examples):
  - EncodeTimeOnly
  - EncodeDateTime

## Notes and Other Information
- The function inverts the sign of the timezone offset for display purposes (tz <= 0 becomes '+')
- Returns a pointer to the new end of the string without NUL termination
- Conditionally includes seconds in output only when non-zero
- Conditionally includes minutes based on style or when non-zero
- Uses zero-padded formatting for consistent output width