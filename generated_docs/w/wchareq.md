# wchareq

## Location
src/backend/utils/adt/like.c: 58 - 93

## Overview
A support routine for MatchText that compares two multibyte character strings as wide characters, returning 1 if they match and 0 otherwise.

## Definition


## Detailed Description
The wchareq function performs byte-by-byte comparison of multibyte character sequences in PostgreSQL's LIKE pattern matching. It's designed to handle Unicode and other multibyte character encodings correctly by:

1. First performing a fast comparison of the first byte as an optimization
2. Checking that both character sequences have the same byte length using pg_mblen()
3. Performing a complete byte-by-byte comparison if lengths match

This function is essential for accurate pattern matching in databases that store text in multibyte encodings, ensuring that character boundaries are respected during comparison operations.

## Parameters / Member Variables
- : Pointer to the first multibyte character sequence to compare
- : Pointer to the second multibyte character sequence to compare

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