# r_CONSONANT

## Location
src/backend/snowball/libstemmer/stem_UTF_8_hindi.c: 297 - 301

## Overview
A static function in the Hindi stemmer that checks if the current cursor position is at a consonant character in the Devanagari Unicode range.

## Definition


## Detailed Description
This function is part of the Snowball-generated Hindi stemming algorithm. It performs a backward grouping test to determine if the character at the current cursor position belongs to the consonant group. The function uses a bit vector () to efficiently test if a Unicode character in the Devanagari range (2325-2399) is classified as a consonant. This is essential for morphological analysis in Hindi text processing, where distinguishing between consonants and vowels is crucial for proper stemming.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the text buffer, cursor position, and other stemming state information

## Dependencies
- Functions called/Symbols referenced:
  - : Tests if character at cursor position belongs to specified Unicode group
  - : Static bit vector defining which characters in range 2325-2399 are consonants
- Called from (representative examples):
  - Currently not called by any other functions in the codebase

## Notes and Other Information
- This is a generated function from Snowball stemmer specification, not hand-written code
- The Unicode range 2325-2399 corresponds to Devanagari consonants in the Unicode standard
- Returns 1 if current position is NOT a consonant (function succeeds when not in grouping)
- Returns 0 if current position IS a consonant (function fails when in grouping) 
- The function operates in backward direction (indicated by  in the called function name)
- Part of the broader Hindi language stemming functionality in PostgreSQL's text search capabilities