# getnum

## Location
src/timezone/localtime.c: 680 - 709

## Overview
The  function extracts and validates an integer from a timezone string, ensuring it falls within specified bounds.

## Definition


## Detailed Description
This static function parses numeric values from timezone strings while performing range validation. It scans consecutive digits starting from the given position, converts them to an integer, and validates that the result falls within the specified minimum and maximum bounds. The function provides early termination if the value exceeds the maximum during parsing to prevent integer overflow. It returns a pointer to the first non-digit character if successful, or NULL if the input is invalid or the number is out of range.

## Parameters / Member Variables
- : Pointer to a position within a timezone string where a number is expected
- : Pointer to an integer where the parsed number will be stored
- : Minimum acceptable value for the number (inclusive)
- : Maximum acceptable value for the number (inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - is_digit (macro/function for digit checking)
- Called from (representative examples):
  - getsecs
  - getrule

## Notes and Other Information
- Returns NULL if the input pointer is NULL, if no digit is found at the current position, or if the number is outside the specified range
- Returns a pointer to the first character after the number if parsing is successful
- Performs overflow protection by checking against the maximum value during parsing rather than after
- The parsed number is stored in the location pointed to by  only if validation succeeds
- Used extensively in timezone rule parsing for extracting hours, minutes, seconds, and day numbers
- Part of the timezone string parsing infrastructure that ensures numeric components are within valid ranges