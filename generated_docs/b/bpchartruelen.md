# bpchartruelen

## Location
src/backend/utils/adt/varchar.c: 676 - 692

## Overview
Computes the true length of a character string by finding the position of the last non-space character, effectively removing trailing spaces from the length calculation.

## Definition
```c
int bpchartruelen(char *s, int len)
```

## Detailed Description
This function implements the core algorithm for determining the "true" length of blank-padded character strings (CHAR type data). It iterates backwards from the end of the string to find the last non-space character, returning the length up to and including that character. This is essential for PostgreSQL's CHAR type semantics where trailing spaces are considered padding and should be ignored in comparisons and other operations. The function assumes that the space character (' ') is a singleton unit in all supported multibyte server encodings, making it safe to perform byte-by-byte comparison.

## Parameters / Member Variables
- `s`: Pointer to the character string data
- `len`: The full length of the string including any trailing spaces

## Dependencies
- Functions called/Symbols referenced:
  - No function calls (uses only basic C operations)
- Called from (representative examples):
  - bcTruelen (wrapper function for BpChar structures)
  - bpcharfastcmp_c (fast comparison function for CHAR types)
  - varstrfastcmp_locale (locale-aware string comparison)
  - varstr_abbrev_convert (abbreviated key conversion for sorting)

## Notes and Other Information
- The algorithm relies on the assumption that space (' ') is a single-byte character in all supported PostgreSQL multibyte encodings
- Returns 0 for strings that consist entirely of spaces or empty strings
- Used internally by many CHAR type operations including comparisons, sorting, and hashing
- Performance-critical function that uses simple backwards iteration for efficiency
- The function handles edge cases like strings shorter than the specified length gracefully