# DecodeTimeCommon

## Location
[src/backend/utils/adt/datetime.c:2590-2671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L2590-L2671)

## Overview
DecodeTimeCommon is a shared time parsing function that decodes time strings with delimiters, supporting both timestamp and interval parsing scenarios with flexible field interpretation.

## Definition

```c
static int
DecodeTimeCommon(char *str, int fmask, int range,
				 int *tmask, struct pg_itm *itm)
```
## Detailed Description
DecodeTimeCommon parses time strings in various formats (HH:MM, HH:MM:SS, MM:SS.sss) and populates a pg_itm structure with the extracted time components. The function demonstrates sophisticated parsing logic that adapts its field interpretation based on context:

1. **Flexible Field Interpretation**: For interval types, the function can reinterpret fields - for example, in MINUTE TO SECOND intervals, "12:34" is parsed as 12 minutes and 34 seconds rather than 12 hours and 34 minutes.

2. **Fractional Second Support**: Handles fractional seconds through integration with ParseFractionalSecond, supporting microsecond precision.

3. **Multiple Format Support**: Accepts formats like HH:MM, HH:MM:SS, HH:MM:SS.sss, and MM:SS.sss, with automatic format detection based on delimiter presence.

4. **Range Validation**: Performs sanity checks on parsed values while allowing the caller to handle hour range validation specific to their context.

The function is designed as a shared utility between timestamp and interval parsing, with the range parameter controlling field reinterpretation behavior.

## Parameters / Member Variables
- `*str`: Input string containing the time to be parsed
- `fmask`: Field mask indicating which fields are already present (input context)
- `range`: Interval range specification that affects field interpretation
- `*tmask`: Output parameter receiving a mask of successfully parsed time fields
- `*itm`: Output parameter receiving the parsed time components (pg_itm structure)
## Dependencies
- Functions called/Symbols referenced:
  - : Converts string to 64-bit integer (for hours)
  - : Converts string to integer (for minutes/seconds)
  - : Parses fractional second components
  - : Macro for creating interval field masks
  - : Time field mask constant
- Called from (representative examples):
  - : Wrapper for timestamp time parsing
  - : Wrapper for interval time parsing

## Notes and Other Information
- Returns 0 for successful parsing, or specific DTERR error codes for various parsing failures
- Uses errno checking with strtoi64/strtoint to detect numeric overflow conditions
- Implements special logic for MINUTE TO SECOND intervals where two-field input is reinterpreted
- Handles fractional seconds with microsecond precision through fsec_t type
- Performs comprehensive range validation for minutes (0-59), seconds (0-60 for leap seconds), and microseconds (0-999999)
- The function is static, indicating it's an internal utility shared between related parsing functions
- Field reinterpretation logic allows the same parsing code to handle both absolute times and interval components

## Simplified Source

```c
static int
DecodeTimeCommon(char *str, int fmask, int range,
                 int *tmask, struct pg_itm *itm)
{
    char *cp;
    fsec_t fsec = 0;

    *tmask = DTK_TIME_M;

    // Parse hours
    errno = 0;
    itm->tm_hour = strtoi64(str, &cp, 10);
    if (errno == ERANGE || *cp != ':')
        return DTERR_FIELD_OVERFLOW;

    // Parse minutes
    errno = 0;
    itm->tm_min = strtoint(cp + 1, &cp, 10);
    if (errno == ERANGE)
        return DTERR_FIELD_OVERFLOW;

    // Handle different time formats
    if (*cp == '\0') {
        // HH:MM format
        itm->tm_sec = 0;
        // Special handling for MINUTE TO SECOND intervals
        if (range == (INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND))) {
            itm->tm_sec = itm->tm_min;
            itm->tm_min = (int)itm->tm_hour;
            itm->tm_hour = 0;
        }
    } else if (*cp == '.') {
        // MM:SS.sss format (fractional seconds)
        if (ParseFractionalSecond(cp, &fsec))
            return DTERR_BAD_FORMAT;
        itm->tm_sec = itm->tm_min;
        itm->tm_min = (int)itm->tm_hour;
        itm->tm_hour = 0;
    } else if (*cp == ':') {
        // HH:MM:SS[.sss] format
        errno = 0;
        itm->tm_sec = strtoint(cp + 1, &cp, 10);
        if (errno == ERANGE)
            return DTERR_FIELD_OVERFLOW;
        if (*cp == '.' && ParseFractionalSecond(cp, &fsec))
            return DTERR_BAD_FORMAT;
    } else {
        return DTERR_BAD_FORMAT;
    }

    // Validate parsed values
    if (itm->tm_hour < 0 ||
        itm->tm_min < 0 || itm->tm_min > MINS_PER_HOUR - 1 ||
        itm->tm_sec < 0 || itm->tm_sec > SECS_PER_MINUTE ||
        fsec < 0 || fsec > USECS_PER_SEC)
        return DTERR_FIELD_OVERFLOW;

    itm->tm_usec = (int)fsec;
    return 0;
}
```