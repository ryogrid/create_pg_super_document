# icu_unicode_version

## Location
src/backend/utils/adt/varlena.c: 6302 - 6315

## Overview
Returns the version of Unicode used by the ICU (International Components for Unicode) library if ICU support is enabled, otherwise returns NULL.

## Definition
```c
Datum icu_unicode_version(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL function that provides information about the Unicode version used by the ICU library linked with PostgreSQL. The behavior of this function depends on whether PostgreSQL was compiled with ICU support:

- **When ICU is enabled** (`USE_ICU` defined): Returns the Unicode version string from ICU's `U_UNICODE_VERSION` constant
- **When ICU is not enabled**: Returns NULL

This conditional compilation approach allows the function to exist in all PostgreSQL builds while providing meaningful information only when ICU support is actually available. This is particularly useful for applications and administrators who need to verify Unicode compatibility between PostgreSQL and ICU.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro (standard PostgreSQL function signature for SQL-callable functions)

## Dependencies
- Functions called/Symbols referenced:
  - `cstring_to_text()` (converts C string to PostgreSQL text type, when ICU enabled)
  - `PG_RETURN_TEXT_P` (macro to return text value, when ICU enabled)
  - `PG_RETURN_NULL()` (macro to return NULL value, when ICU disabled)
  - `U_UNICODE_VERSION` (ICU constant with Unicode version, when ICU enabled)
- Called from (representative examples):
  - `[icu_test](icu_test.md)` in src/common/unicode/category_test.c:136
  - `[icu_test](icu_test.md)` in src/common/unicode/category_test.c:144
  - `[main](../m/main.md)` in src/common/unicode/category_test.c:229

## Notes and Other Information
- This is a SQL-callable function that can be invoked from PostgreSQL queries
- Returns NULL when PostgreSQL is compiled without ICU support (`USE_ICU` not defined)
- When ICU is available, returns the Unicode version in the format provided by ICU's `U_UNICODE_VERSION`
- Useful for verifying Unicode version compatibility between PostgreSQL and ICU
- Primarily used in testing and diagnostic contexts to ensure proper ICU integration
- The function allows applications to detect ICU availability and version information at runtime