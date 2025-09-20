# pgwin32_setlocale

## Location
[src/port/win32setlocale.c:172-193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32setlocale.c#L172-L193)

## Overview
A Windows-specific wrapper around the standard  function that works around two known bugs in the Windows implementation related to locale name handling.

## Definition

```c
char *
pgwin32_setlocale(int category, const char *locale)
```
## Detailed Description
The  function serves as a drop-in replacement for the standard  function on Windows, addressing two specific problems:

1. **Problematic country names**: Windows setlocale() has issues with locale names containing dots in country names (e.g., "Hong Kong S.A.R.", "U.A.E."). The function maps these to acceptable aliases before calling setlocale().

2. **Non-ASCII characters**: The Norwegian (Bokmål) locale name contains non-ASCII characters, which causes encoding issues. The function maps the returned locale name to a pure-ASCII equivalent.

The function applies input mapping using  before calling the real setlocale(), then applies output mapping using  to the return value. This ensures both the input to and output from setlocale() use acceptable locale name formats.

## Parameters / Member Variables
- : The locale category to set (LC_ALL, LC_COLLATE, LC_CTYPE, etc.)
- LANG=C.UTF-8
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL=: The locale string to set, or NULL to query the current locale

## Dependencies
- Functions called/Symbols referenced:
  -  (called twice with different mapping tables)
  -  (standard C library function)
  -  (macro to cast away const qualifier)
  -  (static mapping table for input transformation)
  -  (static mapping table for output transformation)
- Called from:
  - No direct references found (likely used as a replacement for standard setlocale)

## Notes and Other Information
- This is a Windows-specific function that should be used instead of the standard setlocale() on Windows platforms
- The function maintains the same interface as standard setlocale(), making it a drop-in replacement
- Returns a pointer to a string describing the locale, or NULL if the locale cannot be set
- The returned string should not be modified by the caller (standard setlocale() behavior)
- Uses  macro to safely cast away const qualifier from the result of map_locale()
- Handles NULL locale argument by passing it through unchanged to the underlying setlocale()