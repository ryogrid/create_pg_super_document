# builtin_locale_encoding

## Location
[src/backend/utils/adt/pg_locale.c:2547-2567](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L2547-L2567)

## Overview
Returns the required encoding ID for a given builtin locale, or -1 if any encoding is valid for the locale.

## Definition
```c
int builtin_locale_encoding(const char *locale)
```

## Detailed Description
The builtin_locale_encoding function determines the character encoding requirements for PostgreSQL's builtin locale provider. It validates locale names and returns the appropriate encoding identifier that must be used with the specified locale.

The function handles two valid builtin locales:
- "C" locale: Returns -1, indicating any encoding is acceptable
- "C.UTF-8" locale: Returns PG_UTF8, requiring UTF-8 encoding

For any other locale name, the function raises an ERROR, as only these two locales are supported by the builtin provider. This strict validation ensures that only supported locales are used with the builtin collation provider.

## Parameters / Member Variables
- `locale`: String name of the locale to check (e.g., "C", "C.UTF-8")

## Dependencies
- Functions called/Symbols referenced:
  - PG_UTF8
  - strcmp (standard C library function)
  - ereport (for error reporting)
  - [errcode](../e/errcode.md), ERRCODE_WRONG_OBJECT_TYPE
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - [DefineCollation](../D/DefineCollation.md)
  - [builtin_validate_locale](builtin_validate_locale.md)

## Notes and Other Information
- Only supports two builtin locales: "C" and "C.UTF-8"
- Returns -1 for "C" locale to indicate encoding flexibility
- Returns PG_UTF8 constant for "C.UTF-8" locale
- Raises an ERROR for unsupported locale names
- Part of PostgreSQL's builtin collation provider infrastructure
- Used during collation definition and validation processes
- The builtin provider is a simplified alternative to system-dependent locale providers