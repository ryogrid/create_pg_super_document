# pg_get_encoding_from_locale

## Location
[src/port/chklocale.c:306-428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/chklocale.c#L306-L428)

## Overview
Determines the PostgreSQL encoding identifier for a given LC_CTYPE locale setting, handling platform-specific differences and providing fallback mechanisms for unknown encodings.

## Definition

```c
int
pg_get_encoding_from_locale(const char *ctype, bool write_message)
```
## Detailed Description
The pg_get_encoding_from_locale function serves as the primary interface for determining the appropriate PostgreSQL character encoding based on locale settings. This function is crucial for PostgreSQL's internationalization support and proper character handling across different operating systems and locale configurations.

The function operates through several key phases:

**Locale Handling**: 
- If no ctype is provided (NULL), uses the current LC_CTYPE setting
- Handles special cases for "C" and "POSIX" locales by returning PG_SQL_ASCII
- Temporarily sets the locale to extract codeset information, then restores the original setting

**Platform-specific Codeset Extraction**:
- On Unix-like systems: Uses nl_langinfo(CODESET) to get the character set name
- On Windows: Calls win32_langinfo() to extract codepage information in PostgreSQL-compatible format

**Encoding Resolution**:
- Searches through encoding_match_list to find matching PostgreSQL encoding
- Implements platform-specific workarounds (e.g., macOS empty CODESET handling)
- Returns the corresponding PostgreSQL encoding identifier or -1 if not found

**Error Handling**: 
- Can operate in early backend startup when elog() and palloc() may not be available
- Conditionally reports warnings about unrecognized encodings based on write_message parameter

## Parameters / Member Variables
- : LC_CTYPE locale setting to analyze (NULL for current locale, "" for environment-selected)
- : Boolean flag controlling whether to output warning messages for unrecognized encodings

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcasecmp](pg_strcasecmp.md) (case-insensitive string comparison)
  - setlocale (locale manipulation)
  - strdup (string duplication)
  - nl_langinfo (Unix codeset information)
  - [win32_langinfo](../w/win32_langinfo.md) (Windows codepage extraction)
  - encoding_match_list (encoding lookup table)
  - ereport/fprintf (error reporting)
  - PG_SQL_ASCII, PG_UTF8 (PostgreSQL encoding constants)
- Called from (representative examples):
  - [check_encoding_locale_matches](../c/check_encoding_locale_matches.md)
  - [setup_locale_encoding](../s/setup_locale_encoding.md)  
  - [pg_bind_textdomain_codeset](pg_bind_textdomain_codeset.md)
  - [pqConnectOptions2](pqConnectOptions2.md)

## Notes and Other Information
- Returns PG_SQL_ASCII for C/POSIX locales, which callers should treat as compatible with any encoding
- Includes special handling for macOS systems where CODESET may return empty strings but UTF-8 is used
- Designed to work during early backend initialization when memory allocation functions may not be available
- Memory management: Allocates and frees temporary strings for locale manipulation
- Critical for proper database initialization, client connection setup, and locale-aware operations
- Platform differences are abstracted through conditional compilation (#ifndef WIN32, #ifdef __darwin__)
- The function serves as a bridge between system locale settings and PostgreSQL's internal encoding system

## Simplified Source

```c
// Simplified version of pg_get_encoding_from_locale
int pg_get_encoding_from_locale(const char *ctype, bool write_message) {
    char *sys;
    int i;

    // Handle special case: C/POSIX locales allow all encodings
    if (ctype) {
        if (pg_strcasecmp(ctype, "C") == 0 || pg_strcasecmp(ctype, "POSIX") == 0) {
            return PG_SQL_ASCII;
        }
        // Temporarily set locale to extract codeset
        char *save = setlocale(LC_CTYPE, NULL);
        if (!save) return -1;
        save = strdup(save);
        if (!save) return -1;

        char *name = setlocale(LC_CTYPE, ctype);
        if (!name) {
            free(save);
            return -1;
        }

        // Get codeset information (platform-specific)
#ifndef WIN32
        sys = nl_langinfo(CODESET);
        if (sys) sys = strdup(sys);
#else
        sys = win32_langinfo(name);
#endif

        setlocale(LC_CTYPE, save);  // Restore original locale
        free(save);
    } else {
        // Use current locale
        ctype = setlocale(LC_CTYPE, NULL);
        if (!ctype) return -1;

        if (pg_strcasecmp(ctype, "C") == 0 || pg_strcasecmp(ctype, "POSIX") == 0) {
            return PG_SQL_ASCII;
        }

#ifndef WIN32
        sys = nl_langinfo(CODESET);
        if (sys) sys = strdup(sys);
#else
        sys = win32_langinfo(ctype);
#endif
    }

    if (!sys) return -1;

    // Look up encoding in mapping table
    for (i = 0; encoding_match_list[i].system_enc_name; i++) {
        if (pg_strcasecmp(sys, encoding_match_list[i].system_enc_name) == 0) {
            free(sys);
            return encoding_match_list[i].pg_enc_code;
        }
    }

    // Platform-specific workarounds
#ifdef __darwin__
    if (strlen(sys) == 0) {  // macOS empty CODESET = UTF-8
        free(sys);
        return PG_UTF8;
    }
#endif

    // Report warning for unrecognized encoding
    if (write_message) {
#ifdef FRONTEND
        fprintf(stderr, _("could not determine encoding for locale \"%s\": codeset is \"%s\""), ctype, sys);
        fputc('\n', stderr);
#else
        ereport(WARNING, (errmsg("could not determine encoding for locale \"%s\": codeset is \"%s\"", ctype, sys)));
#endif
    }

    free(sys);
    return -1;
}
```

Key simplifications made:
- Streamlined the locale handling logic
- Consolidated error checking patterns
- Preserved platform-specific conditional compilation
- Maintained memory management and error handling
- Added clear comments for each major step
- Kept the essential locale manipulation and encoding lookup logic