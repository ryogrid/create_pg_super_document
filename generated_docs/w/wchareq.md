# wchareq

## Location
[src/backend/utils/adt/like.c:58-93](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like.c#L58-L93)

## Overview
A support routine for MatchText that compares two multibyte character strings as wide characters, returning 1 if they match and 0 otherwise.

## Definition

```c
static inline int
wchareq(const char *p1, const char *p2)
```
## Detailed Description
The wchareq function performs byte-by-byte comparison of multibyte character sequences in PostgreSQL's LIKE pattern matching. It's designed to handle Unicode and other multibyte character encodings correctly by:

1. First performing a fast comparison of the first byte as an optimization
2. Checking that both character sequences have the same byte length using pg_mblen()
3. Performing a complete byte-by-byte comparison if lengths match

This function is essential for accurate pattern matching in databases that store text in multibyte encodings, ensuring that character boundaries are respected during comparison operations.

## Parameters / Member Variables
- `*p1`: Pointer to the first multibyte character sequence to compare
- `*p2`: Pointer to the second multibyte character sequence to compare
## Dependencies
- Functions called/Symbols referenced:
  - [pg_mblen](../p/pg_mblen.md) (used to determine multibyte character length)
- Called from (representative examples):
  - Used via CHAREQ macro in like.c pattern matching routines

## Notes and Other Information
- This is a static inline function, meaning it's optimized for performance and only visible within the like.c compilation unit
- The function includes an optimization that quickly compares the first byte before doing more expensive length calculations
- Previously there was an iwchareq() routine for case-insensitive comparison, but it has been removed
- The function is typically accessed through the CHAREQ(p1, p2) macro defined in the same file
- Critical for proper Unicode and multibyte character support in PostgreSQL's pattern matching operations

## Simplified Source

```c
static inline int
wchareq(const char *p1, const char *p2)
{
    // Quick optimization: compare first byte
    if (*p1 != *p2)
        return 0;

    // Get character lengths and verify they match
    int p1_len = pg_mblen(p1);
    if (pg_mblen(p2) != p1_len)
        return 0;

    // Compare all bytes of the multibyte character
    while (p1_len--) {
        if (*p1++ != *p2++)
            return 0;
    }

    return 1;  // Characters match
}
```