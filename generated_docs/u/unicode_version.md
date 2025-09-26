# unicode_version

## Location
[src/backend/utils/adt/varlena.c:6293-6301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L6293-L6301)

## Overview
Returns the version of Unicode used by PostgreSQL in "major.minor" format as a PostgreSQL text value.

## Definition
```c
Datum unicode_version(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL function that returns the Unicode version used by the PostgreSQL server. It provides version information in "major.minor" format, which matches the format used by ICU (International Components for Unicode).

The function specifically excludes the "update version" (third component) because update versions never involve additions to the character repertoire and are considered unimportant for most practical purposes. This design decision focuses on the semantically significant parts of the Unicode version.

The function uses the `PG_UNICODE_VERSION` compile-time constant to provide the version information, ensuring consistency across the PostgreSQL build.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - `[cstring_to_text](../c/cstring_to_text.md)()` (converts C string to PostgreSQL text type)
  - `PG_UNICODE_VERSION` (compile-time constant with Unicode version)
  - `PG_RETURN_TEXT_P` (macro to return text value from PostgreSQL function)
- Called from (representative examples):
  - No direct references found (likely called via SQL interface)

## Notes and Other Information
- This is a SQL-callable function that can be invoked from PostgreSQL queries
- Returns Unicode version in "major.minor" format (e.g., "15.0")
- The version format matches ICU's Unicode version reporting for consistency
- Excludes update version numbers as they don't affect character repertoire
- Reference documentation: https://unicode.org/versions/
- The actual version returned depends on the `PG_UNICODE_VERSION` constant set during compilation