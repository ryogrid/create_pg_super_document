# surrogate_pair_to_codepoint

## Location
src/include/mb/pg_wchar.h: 553 - 564

## Overview
Converts a UTF-16 surrogate pair (high and low surrogate) into the corresponding Unicode code point value.

## Definition
static inline pg_wchar surrogate_pair_to_codepoint(pg_wchar first, pg_wchar second)

## Detailed Description
This inline function implements the mathematical conversion from a UTF-16 surrogate pair to its corresponding Unicode code point. UTF-16 uses surrogate pairs to encode Unicode characters beyond the Basic Multilingual Plane (code points above U+FFFF). The function takes a high surrogate (first) and low surrogate (second) and combines them using the standard UTF-16 decoding algorithm.

The conversion formula extracts the 10-bit payload from each surrogate (using mask 0x3FF), shifts the high surrogate's payload left by 10 positions, adds the base offset 0x10000 (which represents the start of the supplementary planes), and adds the low surrogate's payload to produce the final code point.

## Parameters / Member Variables
- `first`: The high surrogate (should be in range 0xD800-0xDBFF)
- `second`: The low surrogate (should be in range 0xDC00-0xDFFF)

## Dependencies
- Functions called/Symbols referenced: None (bitwise operations and arithmetic)
- Called from (representative examples):
  - str_udeescape (src/backend/parser/parser.c:434, 473)
  - unistr (src/backend/utils/adt/varlena.c:6547, 6582, 6617)

## Notes and Other Information
- Defined as a static inline function for optimal performance
- Part of the Unicode utility functions in src/include/mb/pg_wchar.h
- The result will be in the range U+10000 to U+10FFFF (supplementary Unicode planes)
- The function assumes valid surrogate pair inputs and does not perform validation
- Uses bit masking (0x3FF) to extract the 10-bit values from each surrogate
- The 0x10000 offset corresponds to the beginning of Plane 1 in Unicode
- Critical for proper handling of emoji, mathematical symbols, and other supplementary Unicode characters
- Used in PostgreSQL's Unicode escape sequence processing and string literal parsing