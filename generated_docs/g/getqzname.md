# getqzname

## Location
[src/timezone/localtime.c:663-679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L663-L679)

## Overview
The  function scans a timezone string until it finds a specified delimiter character, used for parsing quoted or delimited timezone abbreviations.

## Definition

```c
static const char *
getqzname(const char *strp, const int delim)
```
## Detailed Description
This static function extends the functionality of  by allowing parsing of timezone abbreviations that are enclosed within specific delimiters. It scans forward from a given position until it encounters the specified delimiter character or a null terminator. This is particularly useful for parsing extended timezone formats where timezone names may be quoted or otherwise delimited. The function does minimal validation, deferring character set checking to later common-case code for performance reasons.

## Parameters / Member Variables
- : Pointer to a position within a timezone string to begin scanning from
- : The delimiter character to search for (typically a quote or bracket)

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic C operations)
- Called from (representative examples):
  - [tzparse](../t/tzparse.md)

## Notes and Other Information
- Returns a pointer to the delimiter character, or to the null terminator if delimiter is not found
- Used for parsing extended timezone string formats with quoted or bracketed timezone names
- Performs minimal validation - assumes the character set restrictions are checked elsewhere
- Complements  by handling delimited timezone abbreviations rather than stopping at predefined separator characters
- Part of the timezone string parsing infrastructure for handling more complex timezone specifications
- The delimiter parameter allows flexibility in parsing different timezone string formats

## Simplified Source

```c
// Simplified version of getqzname
static const char *
getqzname(const char *strp, const int delim)
{
    // Scan forward until we find the delimiter or end of string
    while (*strp != '\0' && *strp != delim) {
        strp++;
    }

    // Return pointer to delimiter (or null terminator if not found)
    return strp;
}
```

Key simplifications made:
- Removed temporary variable `c` for direct character comparison
- Added explanatory comments for the core scanning logic
- Simplified the while loop condition for better readability
- Made the return purpose explicit with comments