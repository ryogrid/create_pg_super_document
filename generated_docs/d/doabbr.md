# doabbr

## Location
src/timezone/zic.c: 2630 - 2672

## Overview
The doabbr function generates timezone abbreviations from zone format strings, handling various formatting patterns and special cases like offset-based abbreviations and quoted non-alphabetic abbreviations.

## Definition


## Detailed Description
This function creates timezone abbreviations based on the zone's format specification. It handles several different abbreviation formats:

1. **Simple format strings**: Uses sprintf to substitute letters into format patterns
2. **Slash-separated formats**: For zones with different standard/daylight abbreviations (e.g., "EST/EDT")
3. **Offset-based abbreviations**: When format specifier is 'z', generates numeric offset abbreviations
4. **Quoted abbreviations**: Adds angle brackets around non-alphabetic abbreviations for compatibility

The function intelligently handles the daylight saving time context, selecting appropriate parts of slash-separated formats and calculating correct offsets for numeric abbreviations.

## Parameters / Member Variables
- : Output buffer where the generated abbreviation will be written
- : Pointer to the zone structure containing format information
- : Variable part of the abbreviation (e.g., "D" in "EDT")
- : Boolean indicating if this is for daylight saving time
- : Amount of time saved during daylight saving time
- : Whether to add angle brackets around non-alphabetic abbreviations

## Dependencies
- Functions called/Symbols referenced:
  - strchr (to find slash separator in format)
  - [abbroffset](../a/abbroffset.md) (to generate numeric offset strings)
  - sprintf (for format string substitution)
  - strcpy, memcpy (for string copying)
  - strlen (for string length calculation)
  - is_alpha (to check if abbreviation is alphabetic)
  - memmove (for string manipulation when adding quotes)
- Called from (representative examples):
  - [stringzone](../s/stringzone.md) (in src/timezone/zic.c:2900, 2910)
  - [years_of_observations](../y/years_of_observations.md) (in src/timezone/zic.c:3117, 3232, 3243, 3253)

## Notes and Other Information
- Returns the length of the generated abbreviation
- Handles both standard and daylight saving time abbreviations through slash notation
- Automatically quotes non-alphabetic abbreviations with angle brackets when doquotes is true
- Supports the '%z' format specifier for generating offset-based abbreviations
- Used extensively in timezone processing to generate human-readable timezone names
- The function ensures proper null termination of all generated strings