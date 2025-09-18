# SB_lower_char

## Location
src/backend/utils/adt/like.c: 94 - 104

## Overview
A locale-aware character lowercasing function for single-byte encodings used in case-insensitive LIKE pattern matching operations.

## Definition


## Detailed Description
The SB_lower_char function provides optimized case conversion for single-byte character encodings in PostgreSQL's pattern matching system. It handles different locale scenarios:

1. For C locale (locale_is_c = true): Uses fast ASCII-only lowercasing via pg_ascii_tolower()
2. For specific locales: Uses the locale-specific tolower_l() function for proper internationalization
3. For default case: Falls back to PostgreSQL's general pg_tolower() function

This function is part of PostgreSQL's strategy to handle case-insensitive matching efficiently for single-byte encodings while maintaining locale correctness. It's used as a building block in the LIKE pattern matching infrastructure.

## Parameters / Member Variables
- : The unsigned character to convert to lowercase
- LANG=C.UTF-8
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL=: PostgreSQL locale object containing locale-specific information
- : Boolean flag indicating whether the C/POSIX locale is being used

## Dependencies
- Functions called/Symbols referenced:
  - pg_ascii_tolower (fast ASCII lowercasing)
  - tolower_l (locale-specific lowercasing)
  - pg_tolower (PostgreSQL general lowercasing)
  - pg_locale_t (locale type definition)
- Called from (representative examples):
  - MATCH_LOWER macro in single-byte case-insensitive pattern matching

## Notes and Other Information
- This function is part of PostgreSQL's dual strategy for handling case-insensitive matching: fold-on-the-fly for single-byte encodings, and pre-lowercasing for multibyte encodings
- The function provides three different lowercasing strategies depending on locale configuration for optimal performance
- Used through the MATCH_LOWER macro which is defined during compilation of like_match.c for single-byte case-insensitive operations
- The design reflects PostgreSQL's careful attention to both performance and international locale support
- Part of a larger system that abandoned attempts at multibyte case-insensitive comparison due to complexity with functions like tolower() having single-byte APIs