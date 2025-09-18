# asc_tolower

## Location
src/backend/utils/adt/formatting.c: 2158 - 2180

## Overview
A simple ASCII-only lowercase conversion function that converts all uppercase ASCII letters (A-Z) to their lowercase equivalents (a-z) without considering locale or Unicode characters.

## Definition
```c
char *asc_tolower(const char *buff, size_t nbytes)
```

## Detailed Description
The `asc_tolower` function provides fast, ASCII-only lowercase conversion for use in C/POSIX collations where locale-aware or Unicode-aware case conversion is not needed. It operates by making a copy of the input string and then iterating through each byte, applying ASCII lowercase conversion via `pg_ascii_tolower`.

This function is used as a performance optimization for C/POSIX collations, which by definition only deal with ASCII characters and do not require the complexity of locale-aware case conversion. It's called by `str_tolower` when the collation is determined to be C/POSIX via `lc_ctype_is_c`.

The function is safe to use with UTF-8 and other multibyte encodings because `pg_ascii_tolower` only converts ASCII bytes (0x41-0x5A to 0x61-0x7A) and leaves all other bytes unchanged, ensuring that multibyte character sequences remain intact.

## Parameters / Member Variables
- `buff`: Input string buffer to convert (can be null)
- `nbytes`: Number of bytes in the input buffer to process

## Dependencies
- Functions called/Symbols referenced:
  - `pnstrdup`: Create a null-terminated copy of the input buffer
  - `pg_ascii_tolower`: Convert single ASCII character to lowercase
- Called from (representative examples):
  - `str_tolower`: Main collation-aware lowercase function (for C/POSIX collations)
  - `asc_tolower_z`: Null-terminated string wrapper

## Notes and Other Information
- Returns a palloc'd, null-terminated string that must be freed by the caller
- Returns NULL if input buffer is NULL
- Only converts ASCII characters A-Z to a-z; all other bytes are left unchanged
- Safe to use with multibyte encodings as it preserves non-ASCII bytes
- Used specifically for C/POSIX collations where locale-aware conversion is not required
- Provides significant performance benefit over locale-aware functions for ASCII-only use cases
- The function processes the string byte-by-byte rather than character-by-character