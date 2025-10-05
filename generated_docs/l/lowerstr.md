# lowerstr

## Location
[src/backend/tsearch/ts_locale.c:253-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_locale.c#L253-L265)

## Overview
Converts a null-terminated string to lowercase using locale-appropriate case conversion and returns a newly allocated result.

## Definition

```c
char *
lowerstr(const char *str)
```
## Detailed Description
This function is a convenience wrapper around lowerstr_with_len() that handles null-terminated strings. It automatically calculates the string length using strlen() and then delegates to the length-aware version for the actual case conversion. The function handles both single-byte and multi-byte character encodings appropriately.

The underlying implementation (lowerstr_with_len) uses different strategies based on the database encoding: for multi-byte encodings with non-C locales, it converts to wide characters, applies towlower(), and converts back; for single-byte encodings or C locale, it uses simple tolower() on each byte.

## Parameters / Member Variables
- `*str`: Null-terminated input string to convert to lowercase
## Dependencies
- Functions called/Symbols referenced:
  - [lowerstr_with_len](lowerstr_with_len.md)
  - strlen
- Called from (representative examples):
  - [dsnowball_init](../d/dsnowball_init.md)
  - [dispell_init](../d/dispell_init.md)
  - [dsimple_init](../d/dsimple_init.md)
  - [dsynonym_init](../d/dsynonym_init.md)
  - [lowerstr_ctx](lowerstr_ctx.md)
  - [NIImportAffixes](../N/NIImportAffixes.md)

## Notes and Other Information
- Returns a newly palloc'd string that must be freed by the caller
- Handles both single-byte and multi-byte character encodings correctly
- Uses locale-appropriate case conversion rules
- For multi-byte encodings, converts through wide character representation for proper Unicode handling
- For single-byte or C locale, uses simpler byte-by-byte conversion
- Commonly used in text search dictionary initialization for case-insensitive processing
- Part of PostgreSQL's text search infrastructure for normalizing dictionary entries

## Simplified Source

```c
char *lowerstr(const char *str) {
    return lowerstr_with_len(str, strlen(str));
}
```