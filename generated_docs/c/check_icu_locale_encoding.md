# check_icu_locale_encoding

## Location
[src/bin/initdb/initdb.c:2282-2301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2282-L2301)

## Overview
Validates that a user-specified encoding is supported by the ICU (International Components for Unicode) provider.

## Definition

```c
static bool
check_icu_locale_encoding(int user_enc)
```
## Detailed Description
This function ensures that the character encoding specified by the user is compatible with the ICU internationalization library. ICU has specific encoding requirements and limitations, so this validation prevents configuration errors that would cause failures in ICU-based locale operations. The function matches the equivalent validation logic in the backend createdb() function to maintain consistency across PostgreSQL components.

When an incompatible encoding is detected, the function provides clear error messages explaining the issue and suggests corrective actions. This is particularly important when users are configuring PostgreSQL databases with ICU locale providers for advanced internationalization features.

## Parameters / Member Variables
- `user_enc`: The PostgreSQL encoding identifier that the user wants to use with ICU
## Dependencies
- Functions called/Symbols referenced:
  - [is_encoding_supported_by_icu](../i/is_encoding_supported_by_icu.md) (checks if the encoding is compatible with ICU)
  - [pg_encoding_to_char](../p/pg_encoding_to_char.md) (converts encoding ID to human-readable name)
  - pg_log_error (logs error messages)
  - pg_log_error_detail (provides detailed error explanations)
  - pg_log_error_hint (offers suggestions for resolution)
- Called from (representative examples):
  - [setup_locale_encoding](../s/setup_locale_encoding.md) (during ICU locale provider setup)

## Notes and Other Information
- Returns true if the encoding is ICU-compatible, false if incompatible
- Part of the ICU locale provider configuration validation during database initialization
- Provides helpful error messages when encoding conflicts are detected
- Essential for preventing runtime failures with ICU internationalization features
- The validation logic mirrors the backend createdb() function for consistency
- Used specifically when ICU is selected as the locale provider
- Helps users choose appropriate encoding/provider combinations during setup