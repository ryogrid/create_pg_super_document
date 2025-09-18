# icu_language_tag

## Location
src/backend/utils/adt/pg_locale.c: 2944 - 3000

## Overview
Converts a locale string to its BCP47 language tag representation using ICU library functions, with level 2 canonicalization for consistent formatting.

## Definition
```c
char *icu_language_tag(const char *loc_str, int elevel)
```

## Detailed Description
The `icu_language_tag()` function converts locale strings into standardized BCP47 language tags using the ICU library's `uloc_toLanguageTag()` function. This conversion performs "level 2 canonicalization" which provides consistent formatting and can accurately interpret various input locale string formats including POSIX and .NET IDs.

The function is designed to be called before passing locale strings to other ICU functions like `ucol_open()` to ensure proper locale handling. It implements a dynamic buffer allocation strategy since BCP47 language tags don't have a clearly defined upper limit, and older ICU versions may not return the ultimate required length on the first call.

The function is only available when PostgreSQL is compiled with ICU support (`USE_ICU` defined). When ICU is not available, it reports an error and returns NULL.

## Parameters / Member Variables
- `loc_str`: Input locale string to be converted to a BCP47 language tag
- `elevel`: Error level for reporting conversion failures (0 to suppress error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - palloc (PostgreSQL memory allocation)
  - uloc_toLanguageTag (ICU function for locale conversion)
  - repalloc (PostgreSQL memory reallocation)
  - pfree (PostgreSQL memory deallocation)
  - ereport (PostgreSQL error reporting)
  - u_errorName (ICU error name function)
  - MaxAllocSize (PostgreSQL memory limit constant)
  - Min (PostgreSQL minimum macro)
- Called from (representative examples):
  - DefineCollation (src/backend/commands/collationcmds.c:287)
  - pg_import_system_collations (src/backend/commands/collationcmds.c:1002)
  - createdb (src/backend/commands/dbcommands.c:1128)
  - setlocales (src/bin/initdb/initdb.c:2475)

## Notes and Other Information
- Only available when compiled with ICU support (`USE_ICU`)
- Implements dynamic buffer allocation starting with 32 bytes and doubling until MaxAllocSize
- Uses strict mode for BCP47 conversion
- Returns NULL on conversion failure or when ICU is not supported
- Performs level 2 canonicalization for consistent locale representation
- Handles buffer overflow gracefully by reallocating larger buffers
- Part of PostgreSQL's ICU integration for internationalization support