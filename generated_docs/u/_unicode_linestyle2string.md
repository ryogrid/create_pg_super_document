# _unicode_linestyle2string

## Location
src/bin/psql/command.c: 4503 - 4532

## Overview
A utility function in psql that converts enumerated Unicode line style values to their corresponding string representations for display and configuration purposes.

## Definition
```c
static const char *_unicode_linestyle2string(int linestyle)
```

## Detailed Description
The `_unicode_linestyle2string` function serves as a conversion utility that maps integer values representing Unicode line styles to their human-readable string names. This function is the counterpart to `set_unicode_line_style` and is used when psql needs to display the current Unicode line style settings to users. It supports the two Unicode line styles available in psql: single and double line borders for table formatting. The function is essential for user interface purposes where line style settings need to be displayed in help text, configuration output, or status information.

## Parameters / Member Variables
- `linestyle`: An integer value representing the Unicode line style, typically from the unicode_linestyle enumeration

## Dependencies
- Functions called/Symbols referenced:
  - UNICODE_LINESTYLE_SINGLE, UNICODE_LINESTYLE_DOUBLE (enum values for line styles)
- Called from (representative examples):
  - printPsetInfo (for displaying current Unicode line style settings)
  - pset_value_string (for getting line style setting values as strings)

## Notes and Other Information
- The function is declared as static, limiting its scope to the command.c compilation unit
- Returns a default value of "unknown" for any line style values not explicitly handled in the switch statement
- Covers both supported Unicode line styles: single and double borders
- The returned strings are const and should not be modified by the caller
- Used primarily for user interface purposes where line style names need to be displayed
- Part of psql's table formatting system that provides feedback to users about current settings
- This function is used multiple times within printPsetInfo and pset_value_string for different Unicode border components