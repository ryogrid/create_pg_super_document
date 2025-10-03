# check_locale_messages

## Location
[src/backend/utils/adt/pg_locale.c:427-449](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L427-L449)

## Overview
This function validates locale values for the LC_MESSAGES category during PostgreSQL configuration parameter processing, allowing global setting of message locale while handling platform-specific limitations.

## Definition

```c
bool
check_locale_messages(char **newval, void **extra, GucSource source)
```
## Detailed Description
The  function serves as a GUC (Grand Unified Configuration) check hook for validating LC_MESSAGES locale settings. Unlike most other locale categories in PostgreSQL, LC_MESSAGES is allowed to be set globally. The function handles several special cases:

1. **Empty string handling**: Normally empty values are rejected for consistency, but they are accepted when the source is PGC_S_DEFAULT (during startup initialization)
2. **Platform compatibility**: On systems without LC_MESSAGES category or Windows platforms, the function accepts values without validation
3. **Locale validation**: On supported platforms, it delegates to  for actual validation

The function is designed to be permissive during startup to allow environment-based locale settings until the proper configuration can be read from postgresql.conf.

## Parameters / Member Variables
- `**newval`: Pointer to the proposed new locale value string
- `**extra`: Pointer for storing additional data (unused in this function)
- `source`: The source of the configuration change (PGC_S_DEFAULT, file, command line, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - [check_locale](check_locale.md) (for actual locale validation on supported platforms)
  - PGC_S_DEFAULT (configuration source constant)
  - GucSource (enum type for configuration sources)
- Called from (representative examples):
  - GUC system hooks (referenced in guc_hooks.h)

## Notes and Other Information
- The function allows LC_MESSAGES to be set globally, unlike other locale categories in PostgreSQL
- On Windows and systems without LC_MESSAGES, validation is bypassed for compatibility
- Empty string values are specially handled for startup scenarios
- The function is part of PostgreSQL's GUC (Grand Unified Configuration) system
- Platform-specific compilation directives ensure compatibility across different operating systems