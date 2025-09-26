# lowerstr_with_len

## Location
[src/backend/tsearch/ts_locale.c:266-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_locale.c#L266-L333)

## Overview
Converts a string to lowercase with locale-aware processing for multi-byte character encodings, designed for text search functionality in PostgreSQL.

## Definition
```c
char *lowerstr_with_len(const char *str, int len)
```

## Detailed Description
The `lowerstr_with_len` function performs case-folding (conversion to lowercase) on input strings that are not necessarily null-terminated. It intelligently handles both single-byte and multi-byte character encodings based on the database locale settings.

The function implements two distinct processing paths:
1. **Multi-byte path**: Used when `pg_database_encoding_max_length() > 1` and the database ctype is not "C". This path converts the input to wide characters (`wchar_t`), applies `towlower()` for proper Unicode case conversion, then converts back to the database encoding.
2. **Single-byte path**: Used for single-byte encodings or "C" locale. This path uses simple `tolower()` on each byte.

The function is primarily used in PostgreSQL text search functionality to normalize text for case-insensitive matching and indexing.

## Parameters / Member Variables
- `str`: Input string to convert to lowercase (does not need to be null-terminated)
- `len`: Length of the input string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - `[pg_locale_t](../p/pg_locale_t.md)`: Locale type definition
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md): Gets maximum character length for database encoding
  - [char2wchar](../c/char2wchar.md): Converts multi-byte string to wide character string
  - [wchar2char](../w/wchar2char.md): Converts wide character string back to multi-byte string
  - `TOUCHAR`: Macro for safe character conversion
  - [pstrdup](../p/pstrdup.md): PostgreSQL string duplication function
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - [pfree](../p/pfree.md): PostgreSQL memory deallocation function
  - `towlower`: Wide character lowercase conversion function
  - `tolower`: Standard lowercase conversion function

- Called from (representative examples):
  - [dsnowball_lexize](../d/dsnowball_lexize.md): Snowball dictionary lexing function
  - [dispell_lexize](../d/dispell_lexize.md): Ispell dictionary lexing function  
  - [dsimple_lexize](../d/dsimple_lexize.md): Simple dictionary lexing function
  - [dsynonym_lexize](../d/dsynonym_lexize.md): Synonym dictionary lexing function
  - [lowerstr](lowerstr.md): Wrapper function for null-terminated strings
  - `COPYCHAR`: Header macro definition

## Notes and Other Information
- Returns a pallocd string that must be freed by the caller
- Handles edge case of zero-length input by returning empty string
- Uses locale-aware processing to ensure proper case conversion for international characters
- The multi-byte processing path allocates temporary wide character arrays for conversion
- Error handling includes conversion failure detection with appropriate error reporting
- Originally adapted from backend/utils/adt/oracle_compat.c according to code comments
- The TODO comment indicates the locale parameter (mylocale) is not currently utilized but reserved for future enhancement