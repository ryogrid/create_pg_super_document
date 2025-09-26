# is_utf16_surrogate_first

## Location
src/include/mb/pg_wchar.h: 541 - 546

## Overview
Determines whether a given wide character value represents the first (high) surrogate in a UTF-16 surrogate pair.

## Definition
static inline bool is_utf16_surrogate_first(pg_wchar c)

## Detailed Description
This inline function checks if a PostgreSQL wide character value falls within the UTF-16 high surrogate range (0xD800-0xDBFF). In UTF-16 encoding, characters outside the Basic Multilingual Plane (code points above U+FFFF) are represented using surrogate pairs consisting of a high surrogate followed by a low surrogate. The high surrogates occupy the range 0xD800 to 0xDBFF.

This function is essential for proper UTF-16 processing, allowing the system to identify when a character value represents the beginning of a surrogate pair that encodes a single Unicode code point requiring two 16-bit units.

## Parameters / Member Variables
- `c`: The wide character value to test for being a UTF-16 high surrogate

## Dependencies
- Functions called/Symbols referenced: None (simple arithmetic comparison)
- Called from (representative examples):
  - str_udeescape (src/backend/parser/parser.c:443, 482)
  - unistr (src/backend/utils/adt/varlena.c:6556, 6591, 6626)

## Notes and Other Information
- Defined as a static inline function for optimal performance
- Part of the Unicode utility functions in src/include/mb/pg_wchar.h
- The range 0xD800-0xDBFF is reserved exclusively for high surrogates in the Unicode standard
- Must be paired with a corresponding low surrogate (0xDC00-0xDFFF) to form a valid surrogate pair
- Used in PostgreSQL's Unicode escape sequence processing and string literal parsing
- Critical for handling supplementary Unicode characters beyond the Basic Multilingual Plane