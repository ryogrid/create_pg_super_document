# getqzname

## Location
src/timezone/localtime.c: 663 - 679

## Overview
The  function scans a timezone string until it finds a specified delimiter character, used for parsing quoted or delimited timezone abbreviations.

## Definition


## Detailed Description
This static function extends the functionality of  by allowing parsing of timezone abbreviations that are enclosed within specific delimiters. It scans forward from a given position until it encounters the specified delimiter character or a null terminator. This is particularly useful for parsing extended timezone formats where timezone names may be quoted or otherwise delimited. The function does minimal validation, deferring character set checking to later common-case code for performance reasons.

## Parameters / Member Variables
- : Pointer to a position within a timezone string to begin scanning from
- : The delimiter character to search for (typically a quote or bracket)

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic C operations)
- Called from (representative examples):
  - tzparse

## Notes and Other Information
- Returns a pointer to the delimiter character, or to the null terminator if delimiter is not found
- Used for parsing extended timezone string formats with quoted or bracketed timezone names
- Performs minimal validation - assumes the character set restrictions are checked elsewhere
- Complements  by handling delimited timezone abbreviations rather than stopping at predefined separator characters
- Part of the timezone string parsing infrastructure for handling more complex timezone specifications
- The delimiter parameter allows flexibility in parsing different timezone string formats