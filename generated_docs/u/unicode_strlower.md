# unicode_strlower

## Location
[src/common/unicode_case.c:69-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_case.c#L69-L99)

## Overview
Converts a UTF-8 encoded string to lowercase using Unicode case mapping rules and returns the result length.

## Definition
```c
size_t unicode_strlower(char *dst, size_t dstsize, const char *src, ssize_t srclen)
```

## Detailed Description
This function converts an entire UTF-8 encoded string to lowercase using PostgreSQL's Unicode case mapping infrastructure. It processes each Unicode character in the source string, applies lowercase conversion using simple case mapping, and stores the result in the destination buffer.

The function handles UTF-8 encoding properly, converting multi-byte Unicode characters correctly. It can work with both NUL-terminated strings (when srclen < 0) and strings with explicit length. The destination buffer is managed safely with bounds checking to prevent overflow.

## Parameters / Member Variables
- `dst`: Destination buffer for the lowercase string (can be NULL if dstsize is 0)
- `dstsize`: Size of destination buffer in bytes
- `src`: Source UTF-8 encoded string to convert
- `srclen`: Length of source string in bytes, or -1 for NUL-terminated strings

## Dependencies
- Functions called/Symbols referenced:
  - [convert_case](../c/convert_case.md) (internal conversion function)
  - CaseLower (enum value for lowercase conversion)
  - ssize_t (signed size type)
- Called from (representative examples):
  - [str_tolower](../s/str_tolower.md) (formatting functions)
  - [test_strlower](../t/test_strlower.md) (unit tests)

## Notes and Other Information
- Returns the total length of the result string (not including NUL terminator)
- If dstsize is 0, dst may be NULL - useful for calculating required buffer size
- [Result](../R/Result.md) is NUL-terminated only if dstsize is greater than result length
- Properly handles UTF-8 multi-byte character sequences
- Uses PostgreSQL's internal Unicode case mapping table
- Located in src/common/unicode_case.c:69-99
- Safe for buffer size calculation by passing NULL dst and 0 dstsize