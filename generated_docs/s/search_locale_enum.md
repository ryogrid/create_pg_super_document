# search_locale_enum

## Location
[src/backend/utils/adt/pg_locale.c:1060-1125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L1060-L1125)

## Overview
A Windows-specific callback function used by EnumSystemLocalesEx() to search for system locales that match a given language/country specification in English format.

## Definition

```c
static BOOL CALLBACK
search_locale_enum(LPWSTR pStr, DWORD dwFlags, LPARAM lparam)
```
## Detailed Description
The  function serves as a callback for the Windows API function  in the context of locale name resolution. Its purpose is to find a system locale that matches a user-provided locale specification in the format  (e.g., "English" or "English_United States").

The function operates by:
1. Receiving a system locale identifier (pStr) from the Windows enumeration process
2. Using  to retrieve the English language name for the locale
3. Comparing the retrieved name against the target locale specification
4. Handling two comparison scenarios:
   - Simple language-only comparison (when no country is specified)
   - Full language_country comparison (when country is specified)
5. Returning FALSE to stop enumeration when a match is found, TRUE to continue

The function uses a three-element wchar_t array passed via lparam to communicate with the caller: the target locale name, output buffer for the matching locale identifier, and a status flag.

## Parameters / Member Variables
- : Wide character string containing the current system locale identifier being enumerated
- : Flags parameter from EnumSystemLocalesEx (unused in this implementation)
- : Pointer to a wchar_t array containing three elements:
  - : Target locale name to search for (e.g., "English_United States")
  - : Output buffer where matching locale identifier will be stored
  - : Status flag (set to 1 when match found, 0 otherwise)

## Dependencies
- Functions called/Symbols referenced:
  -  (Windows API - retrieves locale information)
  -  (C library - finds last occurrence of character in wide string)
  -  (C library - case-insensitive wide string comparison)
  -  (C library - copies wide string)
  -  (C library - concatenates wide strings)
  -  (C library - calculates wide string length)
  -  (Windows constant)
  -  (Windows constant)
  -  (Windows constant)
- Called from (representative examples):
  -  (via EnumSystemLocalesEx callback mechanism)

## Notes and Other Information
- This function is Windows-specific and only compiled on Windows builds
- Uses Windows API functions for locale information retrieval
- Implements case-insensitive matching using 
- Returns FALSE to stop enumeration when a match is found, TRUE to continue searching
- Handles both language-only matching ("English") and language_country matching ("English_United States")
- The dwFlags parameter is explicitly ignored in this implementation
- Uses wide character (UTF-16) strings throughout for compatibility with Windows locale APIs
- Critical for converting user-friendly locale names to Windows system locale identifiers