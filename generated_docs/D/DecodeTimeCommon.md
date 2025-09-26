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
- : Input string containing the time to be parsed
- : Field mask indicating which fields are already present (input context)
- : Interval range specification that affects field interpretation
- : Output parameter receiving a mask of successfully parsed time fields
- : Output parameter receiving the parsed time components (pg_itm structure)

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