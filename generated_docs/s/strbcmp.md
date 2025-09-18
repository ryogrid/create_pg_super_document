# strbcmp

## Location
src/backend/tsearch/spell.c: 257 - 279

## Overview
A backward string comparison function used for suffix tree operations in PostgreSQL's text search, comparing strings from right to left instead of the standard left-to-right comparison.

## Definition


## Detailed Description
The  function performs a reverse (backward) string comparison, starting from the end of both strings and moving towards the beginning. This specialized comparison is essential for suffix processing in text search operations, where the ending patterns of words are more significant than their beginnings.

The function works by:
1. Finding the length of both strings and starting from the last character of each
2. Comparing characters from right to left using byte values
3. If characters differ, returning the comparison result (-1, 0, or 1)
4. If one string ends before the other, the shorter string is considered "smaller"
5. If all compared characters are equal and strings have the same length, returning 0

This backward comparison is particularly useful for suffix-based affix operations where the ending of the replacement string determines the sorting order and matching behavior.

## Parameters / Member Variables
- : Pointer to the first null-terminated string to compare
- : Pointer to the second null-terminated string to compare

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function)
- Called from:
  - [cmpaffix](../c/cmpaffix.md) (src/backend/tsearch/spell.c:323) - for comparing suffix affixes

## Notes and Other Information
- This is a static function, accessible only within the spell.c compilation unit
- Returns standard comparison function values:
  - Negative value if s1 < s2 (lexicographically, from right to left)
  - Zero if s1 == s2
  - Positive value if s1 > s2
- Used specifically for suffix affixes (FF_SUFFIX type) in the cmpaffix function
- Prefix affixes use regular strcmp(), while suffix affixes use this backward comparison
- The backward comparison ensures that suffixes with similar endings are grouped together during sorting
- Essential for proper suffix tree construction and affix matching in Ispell/Hunspell dictionaries
- Works with byte-level comparison using unsigned char pointers for consistent ordering
- Part of PostgreSQL's text search infrastructure for efficient affix processing