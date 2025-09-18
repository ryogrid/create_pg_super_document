# unicode_strtitle

## Location
src/common/unicode_case.c: 100 - 123

## Overview
Converts a UTF-8 encoded string to titlecase using Unicode case mapping rules and word boundary detection, returning the result length.

## Definition
```c
size_t unicode_strtitle(char *dst, size_t dstsize, const char *src, ssize_t srclen,
                       WordBoundaryNext wbnext, void *wbstate)
```

## Detailed Description
This function converts an entire UTF-8 encoded string to titlecase using PostgreSQL's Unicode case mapping infrastructure and word boundary detection. Titlecasing means capitalizing the first character of each word and making all other characters lowercase. Unlike simple uppercase conversion, titlecase requires knowledge of word boundaries to determine which characters should be capitalized.

The function uses a callback mechanism (wbnext) to identify word boundaries, which allows for proper linguistic titlecasing that respects language-specific word boundary rules. Characters at word boundaries are converted to uppercase, while all other characters are converted to lowercase.

## Parameters / Member Variables
- `dst`: Destination buffer for the titlecase string (can be NULL if dstsize is 0)
- `dstsize`: Size of destination buffer in bytes
- `src`: Source UTF-8 encoded string to convert
- `srclen`: Length of source string in bytes, or -1 for NUL-terminated strings
- `wbnext`: Callback function of type WordBoundaryNext that returns word boundary positions
- `wbstate`: State object for the word boundary callback function

## Dependencies
- Functions called/Symbols referenced:
  - convert_case (internal conversion function)
  - CaseTitle (enum value for titlecase conversion)
  - WordBoundaryNext (callback function type)
  - ssize_t (signed size type)
- Called from (representative examples):
  - [str_initcap](../s/str_initcap.md) (string initialization capitalization functions)

## Notes and Other Information
- Returns the total length of the result string (not including NUL terminator)
- If dstsize is 0, dst may be NULL - useful for calculating required buffer size
- [Result](../R/Result.md) is NUL-terminated only if dstsize is greater than result length
- Requires word boundary callback for proper titlecase conversion
- The wbnext callback should return 0 for first boundary, then each word boundary offset, then total string length for final boundary
- Caller is responsible for initializing and freeing the wbstate callback state
- Uses PostgreSQL's internal Unicode case mapping table
- Located in src/common/unicode_case.c:100-123
- More complex than simple case conversion due to word boundary requirements