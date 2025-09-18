# r_double

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_hungarian.c:542-550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_hungarian.c#L542-L550)

## Overview
The r_double function detects doubled consonants in Hungarian words by testing for specific consonant patterns that appear in doubled form according to Hungarian morphological rules.

## Definition
static int r_double(struct SN_env * z)

## Detailed Description
This function performs a sophisticated test to identify doubled consonants in Hungarian words. It uses bit manipulation and pattern matching to detect specific consonant characters that commonly appear in doubled form in Hungarian morphology.

The function operates through the following mechanism:
1. Saves the current test position using m_test1 = z->l - z->c
2. Performs a bitwise test on the character before the cursor: checks if (z->p[z->c - 1] >> 5) equals 3 and uses a bitmask (106790108) to test specific character patterns
3. Uses find_among_b with array a_2 (containing 23 doubled consonant patterns) to match against predefined doubled consonant sequences
4. Restores the cursor position after the test

This is a non-consuming test function that checks for the presence of doubled consonants without advancing the cursor position, allowing other stemming functions to determine if doubled consonant handling is needed.

## Parameters / Member Variables
- : Pointer to SN_env structure containing:
  - : Current cursor position in the word
  - : Length of the word
  - : Character array containing the word being processed
  - : Lower boundary limit for processing

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward pattern matching function
  - a_2: Array containing 23 doubled consonant patterns
- Called from (representative examples):
  - [r_instrum](r_instrum.md): Hungarian instrumental case processing
  - [r_factive](r_factive.md): Hungarian factive case processing

## Notes and Other Information
- This function is specific to Hungarian morphology where doubled consonants have special significance
- Uses a non-consuming test pattern (m_test1) to avoid advancing the cursor
- The bitmask 106790108 represents a specific set of consonant characters in Hungarian
- The bit manipulation (>> 5 and & 0x1f) efficiently tests character ranges
- Returns 1 if doubled consonants are detected, 0 otherwise
- Essential for proper handling of Hungarian consonant doubling rules in stemming
- Part of the Hungarian morphological analyzer in PostgreSQL's full-text search system