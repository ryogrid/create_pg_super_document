# postgresql_fdw_validator

## Location
[src/backend/foreign/foreign.c:625-680](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L625-L680)

## Overview
Validates generic options given to SERVER or USER MAPPING for PostgreSQL foreign data wrapper, raising errors for invalid options and providing helpful suggestions.

## Definition

```c
struct ConnectionOption *opt;
```
## Detailed Description
The `postgresql_fdw_validator` function is a PostgreSQL SQL-callable function that validates options provided to foreign servers or user mappings in the context of PostgreSQL foreign data wrappers. The function checks each option against the list of valid libpq connection options appropriate for the given context.

When an invalid option is encountered, the function not only reports an error but also attempts to provide a helpful suggestion by finding the closest matching valid option using fuzzy string matching. This improves user experience by guiding users toward correct option names when they make typos or use similar but incorrect option names.

**Important Note**: This function is deprecated and now meant only for testing purposes, as the list of options it knows about may not match those known to the specific libpq instance being used. Modern code should inquire directly from libpq instead.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention providing:
  - First argument: Datum representing the options array to validate
  - Second argument: Oid of the catalog context (server vs user mapping)

## Dependencies
- Functions called/Symbols referenced:
  - [untransformRelOptions](../u/untransformRelOptions.md): Converts options array into internal List format
  - [is_conninfo_option](../i/is_conninfo_option.md): Validates if an option is a valid libpq connection option
  - [initClosestMatch](../i/initClosestMatch.md): Initializes fuzzy string matching state
  - [updateClosestMatch](../u/updateClosestMatch.md): Updates fuzzy matching with candidate option
  - [getClosestMatch](../g/getClosestMatch.md): Retrieves the best matching option name
  - `ereport`: Reports errors with detailed messages and hints
  - [DefElem](../D/DefElem.md): Structure representing option definitions
  - [ConnectionOption](../C/ConnectionOption.md): Structure for libpq connection options
  - [ClosestMatchState](../C/ClosestMatchState.md): State structure for fuzzy string matching
- Called from:
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- **DEPRECATED**: This function is now deprecated and should only be used for testing
- The function validates options against a static list that may be outdated compared to the actual libpq library
- Provides intelligent error reporting with suggestions for similar valid options
- Uses a minimum edit distance of 4 for fuzzy matching suggestions
- Context-aware validation ensures user/password options only appear in appropriate contexts
- Returns true if all options are valid, false (via error) if any invalid option is found
- The error reporting includes helpful hints when valid alternatives exist
- Located in src/backend/foreign/foreign.c:625-680