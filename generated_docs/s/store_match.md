# store_match

## Location
src/backend/regex/regc_pg_locale.c: 722 - 765

## Overview
Adds a character or character range to a character class vector (cvec) in a ctype cache, dynamically expanding storage as needed.

## Definition
```c
static bool store_match(pg_ctype_cache *pcc, pg_wchar chr1, int nchrs)
```

## Detailed Description
This function stores character matching information in a PostgreSQL character type cache. It handles both individual characters and character ranges, automatically managing memory allocation for the storage arrays. When storing a single character, it adds it to the chrs array; when storing multiple characters (a range), it adds the start and end characters to the ranges array.

The function implements dynamic memory management by doubling the allocated space when the current arrays become full. This ensures efficient memory usage while avoiding frequent reallocations during character class construction.

## Parameters / Member Variables
- `pcc`: Pointer to the pg_ctype_cache structure that stores character class information
- `chr1`: The starting character (or single character) to store
- `nchrs`: Number of characters in the range (1 for single character, >1 for ranges)

## Dependencies
- Functions called/Symbols referenced:
  - realloc
- Types referenced:
  - pg_ctype_cache
  - chr
  - cvec
- Called from (representative examples):
  - pg_ctype_get_cache

## Notes and Other Information
- Returns true on success, false if memory allocation fails
- The function is static and only used within the regex locale subsystem
- For ranges (nchrs > 1), stores the start character and end character (chr1 + nchrs - 1) in consecutive array positions
- For single characters (nchrs == 1), stores the character in the chrs array
- Uses a doubling strategy for memory growth to minimize reallocation overhead
- Memory allocation failures are handled gracefully by returning false rather than throwing errors
- The ranges array stores pairs of characters (start, end) for each range
- Used during character class construction to build efficient representations of character sets