# get_collation_actual_version

## Location
src/backend/utils/adt/pg_locale.c: 1752 - 1861

## Overview
Retrieves provider-specific collation version strings from the operating system or collation library to enable version tracking and compatibility checking.

## Definition


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