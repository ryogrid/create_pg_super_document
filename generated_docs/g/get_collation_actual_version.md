# get_collation_actual_version

## Location
[src/backend/utils/adt/pg_locale.c:1752-1861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L1752-L1861)

## Overview
Retrieves provider-specific collation version strings from the operating system or collation library to enable version tracking and compatibility checking.

## Definition

```c
char *
get_collation_actual_version(char collprovider, const char *collcollate)
```
## Detailed Description
This function obtains the actual version string for a collation from the underlying provider (builtin, ICU, or libc). For builtin collations like "C" and "C.UTF-8", it returns a static version "1" since these are memcmp-based and stable. For ICU collations, it queries the ICU library for version information using UCCollator. For libc collations, the implementation is platform-specific: on glibc systems it uses gnu_get_libc_version(), on FreeBSD it uses querylocale() with LC_VERSION_MASK, and on Windows it uses GetNLSVersionEx() API. The function is crucial for detecting when the underlying collation library has been updated, which could affect sort order and require index rebuilds.

## Parameters / Member Variables
- `collprovider`: Character indicating the collation provider (COLLPROVIDER_BUILTIN, COLLPROVIDER_ICU, or COLLPROVIDER_LIBC)
- `collcollate`: String name of the collation to get the version for

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ucol_open](../p/pg_ucol_open.md), ucol_getVersion, ucol_close, u_versionToString (ICU functions)
  - [pg_strcasecmp](../p/pg_strcasecmp.md), pg_strncasecmp (string comparison functions)
  - newlocale, querylocale, freelocale (libc locale functions on FreeBSD)
  - gnu_get_libc_version (glibc function)
  - MultiByteToWideChar, GetNLSVersionEx, GetLastError (Windows functions)
  - [pstrdup](../p/pstrdup.md), psprintf (PostgreSQL memory/string functions)
  - ereport, errcode, errmsg (error reporting)
- Called from (representative examples):
  - [DefineCollation](../D/DefineCollation.md) (at line 360)
  - [AlterCollation](../A/AlterCollation.md) (at line 467)
  - [pg_collation_actual_version](../p/pg_collation_actual_version.md) (at line 573)
  - [createdb](../c/createdb.md) (at lines 1240, 1276)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md) (at line 1707)
  - [CheckMyDatabase](../C/CheckMyDatabase.md) (at line 485)

## Notes and Other Information
- Returns dynamically allocated strings that must be freed by the caller
- For builtin collations (C, C.UTF-8), always returns "1" as they are stable and memcmp-based
- ICU version tracking uses UCCollator version information 
- Platform-specific implementations for libc: glibc uses library version, FreeBSD uses locale version mask, Windows uses NLS version API
- May return NULL if version information is not available (especially on Windows with invalid locale names)
- Critical for collation version mismatch detection and database maintenance operations
- Skips version checking for C-like locales (C, C.*, POSIX) in libc provider as they are considered stable

## Simplified Source

```c
// Simplified version of get_collation_actual_version
char *
get_collation_actual_version(char collprovider, const char *collcollate)
{
    char *collversion = NULL;

    // Handle builtin collations (C and C.UTF-8)
    if (collprovider == COLLPROVIDER_BUILTIN) {
        if (strcmp(collcollate, "C") == 0 || strcmp(collcollate, "C.UTF-8") == 0) {
            return "1";  // Static version for memcmp-based collations
        } else {
            ereport(ERROR, (errmsg("invalid locale name \"%s\" for builtin provider", collcollate)));
        }
    }

#ifdef USE_ICU
    // Handle ICU collations
    if (collprovider == COLLPROVIDER_ICU) {
        UCollator *collator = pg_ucol_open(collcollate);
        UVersionInfo versioninfo;
        char buf[U_MAX_VERSION_STRING_LENGTH];

        ucol_getVersion(collator, versioninfo);
        ucol_close(collator);

        u_versionToString(versioninfo, buf);
        collversion = pstrdup(buf);
    }
    else
#endif
    // Handle libc collations (skip C-like locales)
    if (collprovider == COLLPROVIDER_LIBC &&
        pg_strcasecmp("C", collcollate) != 0 &&
        pg_strncasecmp("C.", collcollate, 2) != 0 &&
        pg_strcasecmp("POSIX", collcollate) != 0) {

        // Platform-specific version retrieval
#if defined(__GLIBC__)
        collversion = pstrdup(gnu_get_libc_version());

#elif defined(LC_VERSION_MASK)
        // FreeBSD implementation
        locale_t loc = newlocale(LC_COLLATE_MASK, collcollate, NULL);
        if (loc) {
            collversion = pstrdup(querylocale(LC_COLLATE_MASK | LC_VERSION_MASK, loc));
            freelocale(loc);
        } else {
            ereport(ERROR, (errmsg("could not load locale \"%s\"", collcollate)));
        }

#elif defined(WIN32)
        // Windows implementation
        NLSVERSIONINFOEX version = {sizeof(NLSVERSIONINFOEX)};
        WCHAR wide_collcollate[LOCALE_NAME_MAX_LENGTH];

        MultiByteToWideChar(CP_ACP, 0, collcollate, -1, wide_collcollate, LOCALE_NAME_MAX_LENGTH);

        if (!GetNLSVersionEx(COMPARE_STRING, wide_collcollate, &version)) {
            if (GetLastError() == ERROR_INVALID_PARAMETER) {
                return NULL;  // Tolerate invalid parameter errors
            }
            ereport(ERROR, (errmsg("could not get collation version for locale \"%s\"", collcollate)));
        }

        collversion = psprintf("%lu.%lu,%lu.%lu",
                              (version.dwNLSVersion >> 8) & 0xFFFF,
                              version.dwNLSVersion & 0xFF,
                              (version.dwDefinedVersion >> 8) & 0xFFFF,
                              version.dwDefinedVersion & 0xFF);
#endif
    }

    return collversion;
}
```

Key simplifications made:
- Consolidated similar string comparisons for builtin collations
- Removed detailed comments within platform-specific sections
- Simplified variable declarations and initialization
- Maintained essential error handling while removing verbose error messages
- Preserved all platform-specific logic paths (#ifdef blocks)
- Kept the core algorithm structure intact for all three providers (builtin, ICU, libc)