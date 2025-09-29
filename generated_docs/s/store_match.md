# store_match

## Location
[src/backend/regex/regc_pg_locale.c:722-765](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_pg_locale.c#L722-L765)

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
  - [pg_ctype_cache](../p/pg_ctype_cache.md)
  - [chr](../c/chr.md)
  - [cvec](../c/cvec.md)
- Called from (representative examples):
  - [pg_ctype_get_cache](../p/pg_ctype_get_cache.md)

## Notes and Other Information
- Returns true on success, false if memory allocation fails
- The function is static and only used within the regex locale subsystem
- For ranges (nchrs > 1), stores the start character and end character (chr1 + nchrs - 1) in consecutive array positions
- For single characters (nchrs == 1), stores the character in the chrs array
- Uses a doubling strategy for memory growth to minimize reallocation overhead
- Memory allocation failures are handled gracefully by returning false rather than throwing errors
- The ranges array stores pairs of characters (start, end) for each range
- Used during character class construction to build efficient representations of character sets

## Simplified Source

```c
static bool
store_match(pg_ctype_cache *pcc, pg_wchar chr1, int nchrs) {
    chr *newchrs;

    if (nchrs > 1) {
        // Handle character range storage
        if (pcc->cv.nranges >= pcc->cv.rangespace) {
            // Expand ranges array by doubling
            pcc->cv.rangespace *= 2;
            newchrs = (chr *) realloc(pcc->cv.ranges,
                                     pcc->cv.rangespace * sizeof(chr) * 2);
            if (newchrs == NULL)
                return false;
            pcc->cv.ranges = newchrs;
        }

        // Store start and end of range
        pcc->cv.ranges[pcc->cv.nranges * 2] = chr1;
        pcc->cv.ranges[pcc->cv.nranges * 2 + 1] = chr1 + nchrs - 1;
        pcc->cv.nranges++;
    } else {
        // Handle single character storage
        if (pcc->cv.nchrs >= pcc->cv.chrspace) {
            // Expand chars array by doubling
            pcc->cv.chrspace *= 2;
            newchrs = (chr *) realloc(pcc->cv.chrs,
                                     pcc->cv.chrspace * sizeof(chr));
            if (newchrs == NULL)
                return false;
            pcc->cv.chrs = newchrs;
        }

        // Store single character
        pcc->cv.chrs[pcc->cv.nchrs++] = chr1;
    }

    return true;
}
```