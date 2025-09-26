# pg_import_system_collations

## Location
[src/backend/commands/collationcmds.c:840-1058](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/collationcmds.c#L840-L1058)

## Overview
A PostgreSQL system function that imports available system collations from the operating system into the pg_collation catalog table for use in database operations.

## Definition
```c
Datum pg_import_system_collations(PG_FUNCTION_ARGS)
```

## Detailed Description
This function discovers and imports collations from multiple sources depending on the platform and compilation options: libc locales (via "locale -a" command), ICU locales (when USE_ICU is defined), and Windows system locales (when ENUM_SYSTEM_LOCALE is defined). It requires superuser privileges and validates that the target namespace exists. For libc locales, it reads from "locale -a" output, creates collations for valid locales, and generates user-friendly aliases (e.g., "en_US" for "en_US.utf8"). For ICU locales, it enumerates available ICU locales and creates collations with "-x-icu" suffix, optionally adding human-readable comments. For Windows systems, it uses EnumSystemLocalesEx() to discover system locales and creates both Windows-style and POSIX-style aliases. The function returns the total number of collations created.

## Parameters / Member Variables
- Function argument 0: `nspid` (Oid) - The namespace OID where collations will be created

## Dependencies
- Functions called/Symbols referenced:
  - [superuser](../s/superuser.md) (permission check)
  - SearchSysCacheExists1 (namespace validation)
  - [OpenPipeStream](../O/OpenPipeStream.md)/ClosePipeStream (execute "locale -a")
  - create_collation_from_locale (create individual collations)
  - [normalize_libc_locale_name](../n/normalize_libc_locale_name.md) (generate aliases)
  - qsort/cmpaliases (sort alias data)
  - [CollationCreate](../C/CollationCreate.md) (create collation catalog entries)
  - [get_collation_actual_version](../g/get_collation_actual_version.md) (version management)
  - [icu_language_tag](../i/icu_language_tag.md) (ICU locale processing)
  - [get_icu_locale_comment](../g/get_icu_locale_comment.md) (ICU locale comments)
  - [win32_read_locale](../w/win32_read_locale.md) (Windows callback function)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md) (transaction management)
- Called from (representative examples):
  - SQL command: SELECT pg_import_system_collations(namespace_oid)

## Notes and Other Information
- Requires superuser privileges - will raise ERROR if called by non-superuser
- Platform-specific behavior controlled by compile-time defines (READ_LOCALE_A_OUTPUT, USE_ICU, ENUM_SYSTEM_LOCALE)
- Creates aliases for easier collation names (e.g., "en_US" instead of "en_US.utf8")
- Handles duplicate collation names gracefully by quietly doing nothing when collations already exist
- For ICU collations, enforces ASCII-only names for template0 compatibility
- Windows implementation creates both Windows-style (hyphenated) and POSIX-style (underscored) collation names
- Returns total count of newly created collations, not including pre-existing ones
- Part of PostgreSQL's internationalization and locale support system
- Warnings are issued if no usable system locales are found on the platform
- Uses expansible arrays for managing large numbers of locale aliases efficiently