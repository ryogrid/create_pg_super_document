# infoname

## Location
src/test/modules/test_regex/test_regex.c: 621 - 691

## Overview
A simple structure that maps regex information flag bits to their textual representations for debugging and testing purposes.

## Definition
```c
struct infoname
{
    int         bit;
    const char *text;
};
```

## Detailed Description
The infoname structure provides a mapping between numeric flag bits from Spencer's regex engine and their corresponding textual names. This structure is used primarily for debugging and testing purposes, allowing the regex testing module to convert numeric flag values into human-readable strings. It's typically used in arrays to create lookup tables for all supported regex information flags.

## Parameters / Member Variables
- `bit`: The numeric value of the regex information flag bit
- `text`: The string representation/name of the flag for display purposes

## Dependencies
- Functions called/Symbols referenced:
  - Various REG_* constants (REG_UBACKREF, REG_ULOOKAROUND, etc.)
  - Used in static array `infonames[]` with entries for all supported flags
- Used by:
  - build_test_info_result function (indirectly through infonames array)
  - Regex testing and debugging functions

## Notes and Other Information
This structure is used to create the static `infonames[]` array that contains mappings for all regex information flags such as REG_UBACKREF, REG_ULOOKAROUND, REG_UBOUNDS, etc. The array is terminated with a {0, NULL} entry. This design pattern allows for easy iteration through all supported flags and provides a centralized location for flag-to-string translations in the regex testing framework.