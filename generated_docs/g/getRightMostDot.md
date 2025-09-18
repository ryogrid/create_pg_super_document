# getRightMostDot

## Location
src/interfaces/ecpg/compatlib/informix.c: 750 - 767

## Overview
A static utility function that finds and returns the position of the rightmost dot (decimal point) in a string, used for numeric formatting operations.

## Definition


## Detailed Description
The `getRightMostDot` function searches through a string from right to left to locate the position of the rightmost dot character ('.'). This is particularly useful in numeric formatting contexts where multiple dots might appear in a format string, and the rightmost one typically indicates the decimal point position.

The function uses a reverse iteration approach, starting from the end of the string and working backwards. When it finds a dot, it calculates and returns the position from the beginning of the string. If no dot is found, it returns -1.

The position calculation uses the formula: `len - j - 1`, where `len` is the string length and `j` is the number of characters traversed from the end.

## Parameters / Member Variables
- `str`: The input string to search for the rightmost dot character

## Dependencies
- Functions called/Symbols referenced:
  - strlen (calculates the length of the input string)
- Called from (representative examples):
  - [rfmtlong](../r/rfmtlong.md) (uses this to determine decimal point positioning in format strings)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Returns the 0-based position of the rightmost dot, or -1 if no dot is found
- Used specifically in the context of the `rfmtlong` function for advanced numeric formatting
- Part of the Informix compatibility layer for handling format strings
- The function searches only for the '.' character, not other potential decimal separators
- Efficient reverse search algorithm that stops at the first (rightmost) dot found