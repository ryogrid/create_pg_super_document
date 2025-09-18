# icu_validate_locale

## Location
[src/backend/utils/adt/pg_locale.c:3001-3081](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L3001-L3081)

## Overview
Performs a best-effort validation check to ensure that a given locale string is valid and supported by the ICU library.

## Definition
```c
void icu_validate_locale(const char *loc_str)
```

## Detailed Description
The `icu_validate_locale()` function validates ICU locale strings by performing several checks to ensure they are valid and can be used with ICU collation functions. The validation process includes:

1. **Language Extraction**: Uses `uloc_getLanguage()` to extract and validate the language component from the locale string
2. **Special Language Check**: Recognizes special language identifiers like empty string, "root", and "und" (undefined)
3. **Language Search**: Searches through all available ICU locales to find a matching language
4. **Collator Test**: Attempts to open a collator with the locale to ensure it can be used

The validation level is controlled by the `icu_validation_level` parameter and can be disabled entirely. During binary upgrades (`pg_upgrade`), error levels are automatically downgraded to warnings to prevent upgrade failures.

The function is only available when PostgreSQL is compiled with ICU support. Without ICU, it reports a feature not supported error.

## Parameters / Member Variables
- `loc_str`: The ICU locale string to validate

## Dependencies
- Functions called/Symbols referenced:
  - uloc_getLanguage (ICU function to extract language from locale)
  - uloc_countAvailable (ICU function to count available locales)
  - uloc_getAvailable (ICU function to get available locale by index)
  - [pg_ucol_open](../p/pg_ucol_open.md) (PostgreSQL wrapper for ICU collator opening)
  - ucol_close (ICU function to close collator)
  - ereport (PostgreSQL error reporting)
  - u_errorName (ICU error name function)
  - strcmp (standard C string comparison)
- Called from (representative examples):
  - [DefineCollation](../D/DefineCollation.md) (src/backend/commands/collationcmds.c:300)
  - [createdb](../c/createdb.md) (src/backend/commands/dbcommands.c:1141)
  - [setlocales](../s/setlocales.md) (src/bin/initdb/initdb.c:2481)

## Notes and Other Information
- Only available when compiled with ICU support (`USE_ICU`)
- Validation can be controlled via `icu_validation_level` parameter and disabled entirely
- During `pg_upgrade`, automatically downgrades error levels to warnings to prevent upgrade failures
- Recognizes special ICU language codes: empty string, "root", and "und" (undefined)
- Searches through all available ICU locales to validate language existence
- Performs actual collator opening test to ensure locale can be used
- Uses `ULOC_LANG_CAPACITY` for language string buffer allocation
- Provides helpful hints on how to disable validation when errors occur
- Part of PostgreSQL's ICU integration for locale validation and internationalization support