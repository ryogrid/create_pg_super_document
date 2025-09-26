# lowerit

## Location
src/timezone/zic.c: 3551 - 3613

## Overview
A locale-independent function that converts ASCII uppercase characters to their lowercase equivalents, used in PostgreSQL's timezone compilation utilities.

## Definition
```c
static char lowerit(char a)
```

## Detailed Description
The `lowerit` function provides a portable way to convert uppercase ASCII letters to lowercase without relying on the standard library's `tolower()` function, which can be affected by locale settings. The function uses a switch statement to explicitly map each uppercase letter (A-Z) to its corresponding lowercase letter (a-z). For any character that is not an uppercase ASCII letter, the function returns the character unchanged.

This implementation ensures consistent behavior across different systems and locale configurations, which is crucial for timezone data processing where predictable string handling is essential.

## Parameters / Member Variables
- `a`: The input character to potentially convert to lowercase

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only built-in language constructs)
- Called from (representative examples):
  - rulesub (src/timezone/zic.c:1848)
  - ciequal (src/timezone/zic.c:3616)
  - itsabbr (src/timezone/zic.c:3625, 3633)
  - ciprefix (src/timezone/zic.c:3645)

## Notes and Other Information
- Returns the lowercase equivalent if the input is an uppercase ASCII letter (A-Z), otherwise returns the input character unchanged
- Locale-independent implementation ensures consistent behavior across different system configurations
- Part of PostgreSQL's timezone compilation utilities (zic)
- Commonly used in case-insensitive string comparison operations within the timezone handling code