# text_position_next_internal

## Location
src/backend/utils/adt/varlena.c: 1400 - 1467

## Overview
The  function implements the raw Boyer-Moore-Horspool string search algorithm, performing efficient byte-level pattern matching.

## Definition


## Detailed Description
The  function is the low-level implementation of the Boyer-Moore-Horspool string searching algorithm in PostgreSQL. It performs raw byte sequence matching without considering multibyte character encoding issues. For single-character needles, it uses a simple linear search. For longer patterns, it implements the full Boyer-Moore-Horspool algorithm, which searches backwards from the end of the potential match and uses a precomputed skip table to efficiently jump over impossible match positions. The algorithm's efficiency comes from being able to skip multiple characters at once when a mismatch is found, based on the character that caused the mismatch.

## Parameters / Member Variables
- : Pointer to the position in the haystack where the search should begin
- : Pointer to TextPositionState structure containing:
  - : Length of the haystack string
  - : Length of the needle pattern  
  - : Pointer to the haystack string
  - : Pointer to the needle pattern
  - : Boyer-Moore-Horspool skip table for efficient searching
  - : Bitmask for skip table indexing

## Dependencies
- Functions called/Symbols referenced:
  - TextPositionState structure fields (haystack, needle, lengths, skip table)
- Called from (representative examples):
  -  - High-level search iteration with multibyte handling

## Notes and Other Information
- Returns pointer to the start of the match, or NULL if no match is found
- Uses linear search optimization for single-character patterns (no skip table benefit)
- Implements backward scanning from the end of potential matches for efficiency
- Skip table allows jumping multiple positions when mismatches occur
- Ignores multibyte encoding issues - caller must validate character boundaries
- Core algorithm powering all PostgreSQL substring search operations
- Uses bit-masking for fast skip table access with variable table sizes
- Highly optimized implementation of a classic string searching algorithm