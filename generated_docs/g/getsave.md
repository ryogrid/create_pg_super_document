# getsave

## Location
src/timezone/zic.c: 1443 - 1470

## Overview
Parses timezone save time values with optional daylight saving time indicators, extracting both the time offset and DST status from formatted strings.

## Definition
```c
static zic_t getsave(char *field, bool *isdst)
```

## Detailed Description
The `getsave` function is a specialized parser for timezone "save" fields that can specify both a time offset and whether that offset represents daylight saving time. It performs the following operations:

1. **DST Indicator Processing**: Examines the last character of the input string for DST indicators:
   - 'd' suffix: Indicates daylight saving time (DST = true)
   - 's' suffix: Indicates standard time (DST = false)
   - No suffix: DST status determined by whether save time is non-zero

2. **Time Parsing**: After removing any DST indicator suffix, delegates the actual time parsing to the `gethms` function to convert the time specification into seconds.

3. **DST Status Determination**: Sets the `isdst` output parameter based on:
   - Explicit 'd' or 's' suffixes when present
   - Non-zero save time implies DST when no explicit indicator is given
   - Zero save time implies standard time when no explicit indicator is given

This function is crucial for processing timezone rule definitions where save times need to specify both the time adjustment and whether that adjustment represents daylight saving time.

## Parameters / Member Variables
- `field`: Input string containing the save time specification (may include 'd' or 's' suffix). Note: This parameter is modified by the function when suffixes are present.
- `isdst`: Output parameter that receives the daylight saving time status (true for DST, false for standard time)

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C string function)
  - [gethms](gethms.md) (time parsing function)
  - zic_t (timezone time type)
- Called from (representative examples):
  - [associate](../a/associate.md) (for parsing zone save times)
  - inrule (for parsing rule save times)

## Notes and Other Information
- This is a static function with internal linkage in src/timezone/zic.c
- **Modifies Input**: The function modifies the input `field` string by removing DST indicator suffixes
- The DST determination logic follows timezone file conventions where unmarked non-zero save times typically indicate daylight saving
- Returns the parsed time offset in seconds using the same units as `gethms`
- Essential for distinguishing between standard time adjustments and daylight saving time adjustments in timezone rules
- The function handles the common timezone file convention where save times can be marked explicitly as standard ('s') or daylight ('d') time