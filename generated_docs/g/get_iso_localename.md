# get_iso_localename

## Location
[src/backend/utils/adt/pg_locale.c:1126-1200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L1126-L1200)

## Overview
Converts a Windows locale name to an ISO-formatted locale identifier, handling both direct matches and system enumeration to find appropriate locale mappings for Visual Studio 2015 or greater.

## Definition

```c
enum, LOCALE_WINDOWS, (LPARAM) argv,
							NULL);
```
## Detailed Description
The  function provides locale name conversion functionality specifically designed for Windows environments using Visual Studio 2015 or later. It transforms Windows-specific locale names into ISO-formatted locale identifiers that can be used with PostgreSQL's locale system.

The function operates through several strategies:
1. **Input Processing**: Strips any codepage suffix (e.g., ".1252") from the input locale name, as GetLocaleInfoEx doesn't handle codepages
2. **Direct Match Attempt**: First tries to get the locale's standard name using GetLocaleInfoEx with LOCALE_SNAME
3. **System Enumeration**: If direct match fails, uses EnumSystemLocalesEx with the search_locale_enum callback to find a matching system locale
4. **Format Conversion**: Converts the result from wide characters to multibyte and replaces hyphens with underscores for Unix-style format

The function handles locale names in the format  and converts them to Unix-style locale identifiers like "en_US" from "en-US" or "English_United States".

## Parameters / Member Variables
- : Input Windows locale name string (e.g., "English_United States" or "en-US.1252")

## Dependencies
- Functions called/Symbols referenced:
  -  (C library - finds character in string)
  -  (PostgreSQL - multibyte string length)
  -  (Windows API - string conversion)
  -  (Windows API - locale information retrieval)
  -  (Windows API - locale enumeration)
  -  (callback function for locale enumeration)
  -  (PostgreSQL - wide char to char conversion)
  -  (Windows constant)
  -  (Windows constant)
  -  (Windows constant)
- Called from (representative examples):
  -  (wrapper function that calls this implementation)

## Notes and Other Information
- This function is Windows-specific and only compiled on Windows builds with Visual Studio 2015 or later
- Returns NULL if no valid conversion is found
- Uses a static buffer  to store the result, making the function non-reentrant
- Strips codepage information from the input as it's not needed for GetLocaleInfoEx
- Converts Windows locale format (hyphen-separated) to Unix format (underscore-separated)
- Does not handle script/variant portions (e.g., uz-Cyrl-UZ) but notes this limitation in comments
- The conversion assumes locale names use only ASCII characters
- Designed to work with PostgreSQL's case-insensitive message catalog filesystem
- Critical for proper locale support on Windows platforms where locale names may differ from Unix standards

## Simplified Source

```c
static char *
get_iso_localename(const char *winlocname)
{
    wchar_t wc_locale_name[LOCALE_NAME_MAX_LENGTH];
    wchar_t buffer[LOCALE_NAME_MAX_LENGTH];
    static char iso_lc_messages[LOCALE_NAME_MAX_LENGTH];
    char *period;
    int len;
    int ret_val;

    // Strip codepage suffix (e.g., ".1252") as GetLocaleInfoEx doesn't handle it
    period = strchr(winlocname, '.');
    if (period != NULL)
        len = period - winlocname;
    else
        len = pg_mbstrlen(winlocname);

    // Convert to wide character format
    memset(wc_locale_name, 0, sizeof(wc_locale_name));
    memset(buffer, 0, sizeof(buffer));
    MultiByteToWideChar(CP_ACP, 0, winlocname, len, wc_locale_name,
                        LOCALE_NAME_MAX_LENGTH);

    // Try direct match with LOCALE_SNAME
    ret_val = GetLocaleInfoEx(wc_locale_name, LOCALE_SNAME, (LPWSTR) &buffer,
                              LOCALE_NAME_MAX_LENGTH);
    if (!ret_val)
    {
        // Search system locales for match
        wchar_t *argv[3];
        argv[0] = wc_locale_name;
        argv[1] = buffer;
        argv[2] = (wchar_t *) &ret_val;
        EnumSystemLocalesEx(search_locale_enum, LOCALE_WINDOWS, (LPARAM) argv, NULL);
    }

    if (ret_val)
    {
        size_t rc;
        char *hyphen;

        // Convert back to multibyte
        rc = wchar2char(iso_lc_messages, buffer, sizeof(iso_lc_messages), NULL);
        if (rc == -1 || rc == sizeof(iso_lc_messages))
            return NULL;

        // Replace hyphen with underscore for Unix-style format
        hyphen = strchr(iso_lc_messages, '-');
        if (hyphen)
            *hyphen = '_';
        return iso_lc_messages;
    }

    return NULL;
}
```