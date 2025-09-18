# pg_strcasecmp

## Location
src/port/pgstrcasecmp.c: 36 - 68

## Overview
Performs case-independent comparison of two null-terminated strings, providing locale-aware case conversion for both ASCII and extended character sets.

## Definition


## Detailed Description
The  function compares two null-terminated strings character by character in a case-insensitive manner. It handles both ASCII characters (A-Z) and locale-specific extended characters with high-bit set. For ASCII characters, it performs direct case conversion by adding the offset between uppercase and lowercase letters. For extended characters (those with the high bit set), it uses the standard C library's  and  functions to handle locale-specific case conversions properly.

The function iterates through both strings simultaneously, converting characters to lowercase before comparison. If characters differ after case conversion, it returns the difference. If one string ends before the other, the comparison reflects the shorter string as "less than" the longer one. The function returns 0 when strings are equal (case-insensitively).

## Parameters / Member Variables
- : First null-terminated string to compare
- : Second null-terminated string to compare

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if character has high bit set)
  - isupper (standard C library function for locale-aware uppercase detection)
  - tolower (standard C library function for locale-aware case conversion)
- Called from (representative examples):
  - Used extensively throughout PostgreSQL codebase for case-insensitive string comparisons
  - Configuration parameter processing (guc.c)
  - [Command](../C/Command.md) parsing and SQL keyword matching
  - Authentication and connection handling
  - Utility functions in psql and other tools

## Notes and Other Information
- Returns negative value if s1 < s2, positive if s1 > s2, zero if equal
- Optimized for ASCII characters with direct case conversion
- Falls back to locale-aware functions for extended character sets
- Widely used throughout PostgreSQL for configuration options, SQL keywords, and user input processing
- Part of PostgreSQL's portability layer to ensure consistent behavior across platforms