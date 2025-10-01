# cache_locale_time

## Location
[src/backend/utils/adt/pg_locale.c:829-1059](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L829-L1059)

## Overview
Updates the localization cache for time-related locale data by extracting localized day and month names using strftime() and converting them to the database encoding for efficient reuse.

## Definition

```c
struct tm  *timeinfo;
```
## Detailed Description
The  function is responsible for populating PostgreSQL's internal cache of localized time strings (day and month names in both abbreviated and full forms). This function is critical for performance of date/time formatting operations, as it avoids repeatedly calling expensive locale functions.

The function operates in several phases:
1. **Validation**: Checks if caching has already been performed via 
2. **Locale Setup**: Temporarily sets LC_TIME (and LC_CTYPE on Windows) to the target locale
3. **Data Extraction**: Uses strftime() to extract localized day names (%a, %A) and month names (%b, %B)
4. **Encoding Conversion**: Converts the extracted strings from the locale encoding to the database encoding
5. **Caching**: Stores the converted strings in global arrays using 
6. **Cleanup**: Restores original locale settings and marks the cache as valid

The function handles platform differences, particularly Windows where  is used and always returns UTF-8 data, while on other platforms the encoding must be determined from the locale.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables and arrays:
- : Flag indicating if cache is valid
- : Current time locale setting
- : Cached abbreviated day names
- : Cached full day names  
- : Cached abbreviated month names
- : Cached full month names

## Dependencies
- Functions called/Symbols referenced:
  -  (C library function for locale management)
  -  and  (C library time functions)
  -  (or  on Windows)
  -  (PostgreSQL encoding detection)
  -  (helper function for string conversion and caching)
  -  (PostgreSQL logging)
  -  and  (PostgreSQL memory management)
  -  (buffer size constant)
- Called from (representative examples):
  -  (date/time formatting function)
  -  (date/time parsing function)

## Notes and Other Information
- The function is designed to be called only once per locale change, with  preventing redundant work
- Uses a large stack buffer (2 * 7 + 2 * 12) * MAX_L10N_DATA to collect all strftime results before processing
- Critical section design ensures locale settings are always restored even if errors occur
- On Windows, LC_CTYPE must also be set because wcsftime() internally uses it
- The function handles encoding conversion differently on Windows (assumes UTF-8) vs other platforms (detects from locale)
- Error handling includes checking for strftime failures and locale restoration failures (marked as FATAL)
- Thread safety considerations: temporarily modifies process-wide locale settings
- The cached data persists in TopMemoryContext for the lifetime of the backend process

## Simplified Source

```c
void
cache_locale_time(void)
{
    // Return early if cache is already valid
    if (CurrentLCTimeValid)
        return;

    // Save current locale settings
    char *save_lc_time = setlocale(LC_TIME, NULL);
    save_lc_time = pstrdup(save_lc_time);

#ifdef WIN32
    char *save_lc_ctype = setlocale(LC_CTYPE, NULL);
    save_lc_ctype = pstrdup(save_lc_ctype);
    setlocale(LC_CTYPE, locale_time);
#endif

    // Set target locale for time formatting
    setlocale(LC_TIME, locale_time);

    // Extract localized day and month names using strftime
    char buf[(2 * 7 + 2 * 12) * MAX_L10N_DATA];
    char *bufptr = buf;
    time_t timenow = time(NULL);
    struct tm *timeinfo = localtime(&timenow);
    bool strftimefail = false;

    // Get localized day names (abbreviated and full)
    for (int i = 0; i < 7; i++) {
        timeinfo->tm_wday = i;
        if (strftime(bufptr, MAX_L10N_DATA, "%a", timeinfo) <= 0)
            strftimefail = true;
        bufptr += MAX_L10N_DATA;
        if (strftime(bufptr, MAX_L10N_DATA, "%A", timeinfo) <= 0)
            strftimefail = true;
        bufptr += MAX_L10N_DATA;
    }

    // Get localized month names (abbreviated and full)
    for (int i = 0; i < 12; i++) {
        timeinfo->tm_mon = i;
        timeinfo->tm_mday = 1;  // ensure valid date
        if (strftime(bufptr, MAX_L10N_DATA, "%b", timeinfo) <= 0)
            strftimefail = true;
        bufptr += MAX_L10N_DATA;
        if (strftime(bufptr, MAX_L10N_DATA, "%B", timeinfo) <= 0)
            strftimefail = true;
        bufptr += MAX_L10N_DATA;
    }

    // Restore original locale settings (critical for cleanup)
#ifdef WIN32
    setlocale(LC_CTYPE, save_lc_ctype);
    pfree(save_lc_ctype);
#endif
    setlocale(LC_TIME, save_lc_time);
    pfree(save_lc_time);

    // Check for strftime errors
    if (strftimefail)
        elog(ERROR, "strftime() failed: %m");

    // Convert and cache the extracted strings
    int encoding = PG_UTF8;  // Windows assumption
#ifndef WIN32
    encoding = pg_get_encoding_from_locale(locale_time, true);
    if (encoding < 0)
        encoding = PG_SQL_ASCII;
#endif

    // Cache day and month names in global arrays
    bufptr = buf;
    for (int i = 0; i < 7; i++) {
        cache_single_string(&localized_abbrev_days[i], bufptr, encoding);
        bufptr += MAX_L10N_DATA;
        cache_single_string(&localized_full_days[i], bufptr, encoding);
        bufptr += MAX_L10N_DATA;
    }
    for (int i = 0; i < 12; i++) {
        cache_single_string(&localized_abbrev_months[i], bufptr, encoding);
        bufptr += MAX_L10N_DATA;
        cache_single_string(&localized_full_months[i], bufptr, encoding);
        bufptr += MAX_L10N_DATA;
    }

    // Mark cache as valid
    CurrentLCTimeValid = true;
}
```