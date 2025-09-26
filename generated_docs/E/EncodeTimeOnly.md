# EncodeTimeOnly

## Location
src/backend/utils/adt/datetime.c: 4312 - 4341

## Overview
EncodeTimeOnly formats time components (hours, minutes, seconds, fractional seconds) into a string representation, optionally including timezone information.

## Definition

```c
void
EncodeTimeOnly(struct pg_tm *tm, fsec_t fsec, bool print_tz, int tz, int style, char *str)
```
## Detailed Description
EncodeTimeOnly converts time-related components from a pg_tm structure and fractional seconds into a formatted time string. The function formats time in HH:MM:SS format with optional fractional seconds and timezone offset. It uses zero-padded formatting for hours and minutes, delegates seconds formatting to AppendSeconds for proper fractional second handling, and conditionally includes timezone information based on the print_tz flag. The output format is suitable for PostgreSQL's time and timetz data types.

## Parameters / Member Variables
- : Pointer to pg_tm structure containing time components (hour, minute, second)
- : Fractional seconds component (microsecond precision)
- : Boolean flag indicating whether to include timezone information in output
- : Timezone offset in seconds (used when print_tz is true)
- : Date/time formatting style (passed to EncodeTimezone when needed)
- : Destination string buffer where the formatted time will be written (NUL-terminated)

## Dependencies
- Functions called/Symbols referenced:
  - pg_ultostr_zeropad (for zero-padded hour and minute formatting)
  - AppendSeconds (for seconds and fractional seconds formatting)
  - EncodeTimezone (for timezone offset formatting when print_tz is true)
  - MAX_TIME_PRECISION (constant for maximum time precision)
  - fsec_t (type for fractional seconds)
- Called from (representative examples):
  - time_out
  - timetz_out
  - JsonEncodeDateTime

## Notes and Other Information
- Always formats time in HH:MM:SS format with colon separators
- Uses AppendSeconds for proper handling of fractional seconds up to MAX_TIME_PRECISION
- Timezone formatting is conditional based on print_tz flag (difference between time and timetz types)
- Always NUL-terminates the output string
- Delegates timezone formatting to EncodeTimezone function for consistency
- Suitable for both time and timetz PostgreSQL data types depending on print_tz setting