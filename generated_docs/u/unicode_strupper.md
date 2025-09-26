# unicode_strupper

## Location
[src/common/unicode_case.c:124-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_case.c#L124-L136)

## Overview
Converts a UTF-8 encoded string to uppercase and returns the result length without the terminating NUL.

## Definition

```c
size_t
unicode_strupper(char *dst, size_t dstsize, const char *src, ssize_t srclen)
```
## Detailed Description
The  function is a wrapper around  that specifically handles uppercase conversion of UTF-8 strings. It provides a simplified interface for converting strings to uppercase by calling the more general  function with  as the case kind parameter.

The function handles proper Unicode case conversion according to Unicode standards, not just ASCII characters. It can work with both NUL-terminated strings (when ) and strings with explicit length. The destination buffer can be smaller than needed, in which case the output will be truncated but the function still returns the full result length, allowing callers to determine the required buffer size.

## Parameters / Member Variables
- : Destination buffer to store the uppercase result. Can be NULL if dstsize is zero
- : Size of the destination buffer. If zero, dst may be NULL for size calculation
- : Source UTF-8 encoded string to convert. Must be NUL-terminated if srclen < 0
- : Length of source string in bytes, or negative for NUL-terminated strings

## Dependencies
- Functions called/Symbols referenced:
  - [convert_case](../c/convert_case.md) (performs the actual case conversion)
  - ssize_t (POSIX type for signed size values)
- Called from (representative examples):
  - [str_toupper](../s/str_toupper.md) (in src/backend/utils/adt/formatting.c)

## Notes and Other Information
- This is a convenience wrapper that simplifies uppercase conversion by pre-setting the case kind
- The function returns the length of the result (excluding NUL terminator) even if the destination buffer was too small
- Proper Unicode case conversion is performed, handling multi-byte UTF-8 sequences correctly
- If dstsize is greater than the result length, the destination will be NUL-terminated; otherwise it will not be
- Useful for calculating required buffer size by calling with dst=NULL and dstsize=0