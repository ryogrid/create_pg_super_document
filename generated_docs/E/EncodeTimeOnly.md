# EncodeTimeOnly

## Location
[src/backend/utils/adt/datetime.c:4312-4341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L4312-L4341)

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
  - [pg_ultostr_zeropad](../p/pg_ultostr_zeropad.md) (for zero-padded hour and minute formatting)
  - [AppendSeconds](../A/AppendSeconds.md) (for seconds and fractional seconds formatting)
  - [EncodeTimezone](EncodeTimezone.md) (for timezone offset formatting when print_tz is true)
  - MAX_TIME_PRECISION (constant for maximum time precision)
  - fsec_t (type for fractional seconds)
- Called from (representative examples):
  - [time_out](../t/time_out.md)
  - [timetz_out](../t/timetz_out.md)
  - [JsonEncodeDateTime](../J/JsonEncodeDateTime.md)

## Notes and Other Information
- Always formats time in HH:MM:SS format with colon separators
- Uses AppendSeconds for proper handling of fractional seconds up to MAX_TIME_PRECISION
- Timezone formatting is conditional based on print_tz flag (difference between time and timetz types)
- Always NUL-terminates the output string
- Delegates timezone formatting to EncodeTimezone function for consistency
- Suitable for both time and timetz PostgreSQL data types depending on print_tz setting

## Simplified Source

```c
void EncodeTimeOnly(struct pg_tm *tm, fsec_t fsec, bool print_tz, int tz, int style, char *str) {
    // Format hours with zero padding (HH)
    str = pg_ultostr_zeropad(str, tm->tm_hour, 2);
    *str++ = ':';

    // Format minutes with zero padding (MM)
    str = pg_ultostr_zeropad(str, tm->tm_min, 2);
    *str++ = ':';

    // Format seconds and fractional seconds (SS.ssssss)
    str = AppendSeconds(str, tm->tm_sec, fsec, MAX_TIME_PRECISION, true);

    // Include timezone offset if requested (for timetz type)
    if (print_tz)
        str = EncodeTimezone(str, tz, style);

    // Null-terminate the string
    *str = '\0';
}
```