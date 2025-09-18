# pg_encoding_mblen_bounded

## Location
src/common/wchar.c: 2167 - 2175

## Overview
Returns the byte length of a multibyte character, bounded by the distance to the terminating zero byte to prevent reading beyond string boundaries.

## Definition
```c
int pg_encoding_mblen_bounded(int encoding, const char *mbstr)
```

## Detailed Description
This function provides a safe way to determine the byte length of a multibyte character by ensuring that the returned length does not exceed the distance to the terminating zero byte. It combines the functionality of `pg_encoding_mblen()` with boundary checking using `strnlen()` to prevent buffer overruns when working with zero-terminated strings.

The function is particularly useful when dealing with multibyte character encodings where character lengths can vary, and you need to ensure that character length calculations don't extend beyond the actual string boundaries. For input that might lack a terminating zero, the documentation recommends using `Min(remaining, pg_encoding_mblen_or_incomplete())` instead.

## Parameters / Member Variables
- `encoding`: The character encoding identifier (e.g., UTF8, Latin1, etc.)
- `mbstr`: Pointer to the multibyte character string to analyze

## Dependencies
- Functions called/Symbols referenced:
  - `[pg_encoding_mblen](pg_encoding_mblen.md)`: Gets the theoretical byte length of a multibyte character
  - `strnlen`: Standard C library function to find string length with boundary limit

- Called from (representative examples):
  - No direct callers found in the current codebase

## Notes and Other Information
- This function is defined in src/common/wchar.c:2167-2175
- It's designed specifically for zero-terminated strings where boundary safety is paramount
- The function effectively implements: `strnlen(mbstr, pg_encoding_mblen(encoding, mbstr))`
- For non-zero-terminated input, consider using `pg_encoding_mblen_or_incomplete()` with explicit length limits
- Part of PostgreSQL's character encoding handling infrastructure in the common utilities