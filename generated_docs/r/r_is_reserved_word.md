# r_is_reserved_word

## Location
src/backend/snowball/libstemmer/stem_UTF_8_turkish.c: 2005 - 2015

## Overview
Checks if the current word is a Turkish reserved word that should not be stemmed, specifically detecting the words "ad" and "soyad" (name and surname).

## Definition


## Detailed Description
This function implements a reserved word check for the Turkish stemmer to prevent stemming of certain important words that should remain unchanged. The function specifically checks for:

1. Words ending with "ad" (Turkish word for "name")
2. Optionally preceded by "soy" to form "soyad" (Turkish word for "surname")
3. Ensures the match occurs at the beginning of the word (cursor at left boundary)

The function uses backward string matching to detect these patterns and returns 1 if a reserved word is found, preventing further stemming operations on these words.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the string being processed and cursor positions

## Dependencies
- Functions called/Symbols referenced:
  - eq_s_b (Snowball function for backward string equality testing)
- Called from:
  - r_postlude (at src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:2043)

## Notes and Other Information
- This is a static function within the Turkish stemmer implementation
- Returns 1 if a reserved word is detected, 0 otherwise
- Critical for preserving important Turkish words like personal name-related terms
- Part of the final validation phase in Turkish word stemming
- Generated automatically by Snowball 2.2.0 stemmer generator
- Helps maintain semantic meaning by preventing over-stemming of culturally significant words