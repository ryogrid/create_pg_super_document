# builtin_validate_locale

## Location
[src/backend/utils/adt/pg_locale.c:2568-2601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L2568-L2601)

## Overview
Validates the locale and encoding combination for the builtin provider and returns the canonical form of the locale name.

## Definition
```c
const char *builtin_validate_locale(int encoding, const char *locale)
```

## Detailed Description
The builtin_validate_locale function performs comprehensive validation of locale and encoding combinations for PostgreSQL's builtin collation provider. It serves two primary purposes: validating that the locale is supported by the builtin provider and ensuring the encoding is compatible with the specified locale.

The function recognizes and canonicalizes locale names:
- "C" remains as "C" (canonical form)
- "C.UTF-8" and "C.UTF8" both canonicalize to "C.UTF-8"

After determining the canonical locale name, it validates the encoding compatibility by calling builtin_locale_encoding() to get the required encoding for the locale. If the locale requires a specific encoding and the provided encoding doesn't match, an error is raised.

This validation is crucial for maintaining data integrity and proper collation behavior in databases using the builtin provider.

## Parameters / Member Variables
- `encoding`: The encoding ID to validate against the locale
- `locale`: The locale name string to validate and canonicalize

## Dependencies
- Functions called/Symbols referenced:
  - [builtin_locale_encoding](builtin_locale_encoding.md)
  - [pg_encoding_to_char](../p/pg_encoding_to_char.md)
  - strcmp (standard C library function)
  - ereport (for error reporting)
  - [errcode](../e/errcode.md), ERRCODE_WRONG_OBJECT_TYPE
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - [DefineCollation](../D/DefineCollation.md)
  - [createdb](../c/createdb.md)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md)
  - [CheckMyDatabase](../C/CheckMyDatabase.md)

## Notes and Other Information
- Only supports "C" and "C.UTF-8"/"C.UTF8" locales for builtin provider
- Canonicalizes "C.UTF8" to "C.UTF-8" for consistency
- Performs encoding compatibility validation by checking against required encoding
- Returns the canonical locale name on successful validation
- Raises ERROR for unsupported locales or encoding mismatches
- Used during database creation, collation definition, and locale initialization
- Part of PostgreSQL's builtin collation provider infrastructure
- Essential for ensuring locale/encoding consistency across the system

## Simplified Source

```c
// Simplified version of builtin_validate_locale
const char *
builtin_validate_locale(int encoding, const char *locale)
{
    const char *canonical_name = NULL;

    // Step 1: Validate and canonicalize locale name
    if (strcmp(locale, "C") == 0)
        canonical_name = "C";
    else if (strcmp(locale, "C.UTF-8") == 0 || strcmp(locale, "C.UTF8") == 0)
        canonical_name = "C.UTF-8";

    // Step 2: Reject unsupported locales
    if (!canonical_name)
        ereport(ERROR, "invalid locale name for builtin provider");

    // Step 3: Validate encoding compatibility
    int required_encoding = builtin_locale_encoding(canonical_name);
    if (required_encoding >= 0 && encoding != required_encoding)
        ereport(ERROR, "encoding does not match locale");

    return canonical_name;
}
```

Key simplifications made:
- Simplified error reporting by removing detailed error codes and message formatting
- Combined related conditions into clear logical steps
- Removed verbose parameter passing in ereport calls
- Consolidated locale validation logic into clear branches
- Maintained essential validation flow while improving readability