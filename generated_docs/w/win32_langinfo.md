# win32_langinfo

## Location
[src/port/chklocale.c:202-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/chklocale.c#L202-L269)

## Overview
A Windows-specific helper function that extracts codepage information from locale strings to convert them into PostgreSQL-compatible encoding names.

## Definition

```c
static char *
win32_langinfo(const char *ctype)
```
## Detailed Description
The win32_langinfo function serves as a Windows-specific implementation for extracting codepage information from locale strings. On Windows systems, instead of using the standard nl_langinfo() result, this function parses locale strings to determine the appropriate codepage and returns it in a format that PostgreSQL can use for character encoding.

The function implements a two-tier approach:
1. **Modern approach (Visual Studio 2010+)**: Uses GetLocaleInfoEx() to parse short locale names like "de-DE", "fr-FR" by converting them to wide characters and querying the system for the default ANSI codepage.
2. **Fallback approach**: For older compilers or when GetLocaleInfoEx() fails, it manually parses locale strings in the format <Language>_<Country>.<CodePage> (e.g., "English_United States.1252") by extracting the codepage number after the last dot.

The function handles special cases such as CP_ACP (no ANSI codepage available, returns "utf8") and Unix-style locale strings that Windows' setlocale() accepts but GetLocaleInfoEx() doesn't recognize.

## Parameters / Member Variables
- : Input locale string to be parsed for codepage information

## Dependencies
- Functions called/Symbols referenced:
  - malloc (for memory allocation)
  - MultiByteToWideChar (Windows API for character conversion)
  - GetLocaleInfoEx (Windows API for locale information)
  - strrchr (string manipulation)
  - strlen (string length)
  - strspn (string span)
  - sprintf (formatted string creation)
  - strcpy (string copying)
- Called from (representative examples):
  - [pg_get_encoding_from_locale](../p/pg_get_encoding_from_locale.md)

## Notes and Other Information
- This is a static function specific to Windows builds and is conditionally compiled
- Returns a malloc()'d string that must be freed by the caller
- The function prioritizes GetLocaleInfoEx() when available (Visual Studio 2010+) but falls back to manual parsing for compatibility
- Handles the special case where CP_ACP indicates only Unicode is available for the locale
- Part of PostgreSQL's cross-platform locale handling system, specifically addressing Windows' different approach to codepage representation

## Simplified Source

```c
static char *win32_langinfo(const char *ctype)
{
    char *result = NULL;
    char *codepage;

#if defined(_MSC_VER)
    // Modern approach: Use GetLocaleInfoEx for VS 2010+
    uint32 cp;
    WCHAR wctype[LOCALE_NAME_MAX_LENGTH];

    // Convert input to wide chars
    memset(wctype, 0, sizeof(wctype));
    MultiByteToWideChar(CP_ACP, 0, ctype, -1, wctype, LOCALE_NAME_MAX_LENGTH);

    // Try to get codepage from system
    if (GetLocaleInfoEx(wctype,
                        LOCALE_IDEFAULTANSICODEPAGE | LOCALE_RETURN_NUMBER,
                        (LPWSTR) &cp, sizeof(cp) / sizeof(WCHAR)) > 0)
    {
        result = malloc(16);
        if (result != NULL)
        {
            // Handle special case: no ANSI codepage available
            if (cp == CP_ACP)
                strcpy(result, "utf8");
            else
                sprintf(result, "CP%u", cp);
        }
    }
    else
#endif
    {
        // Fallback: Parse locale string manually
        // Format: <Language>_<Country>.<CodePage>
        codepage = strrchr(ctype, '.');
        if (codepage != NULL)
        {
            codepage++;
            size_t ln = strlen(codepage);
            result = malloc(ln + 3);
            if (result != NULL)
            {
                // Check if codepage is all digits
                if (strspn(codepage, "0123456789") == ln)
                    sprintf(result, "CP%s", codepage);
                else
                    strcpy(result, codepage);
            }
        }
    }

    return result;
}
```