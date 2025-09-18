# r_VOWEL

## Location
src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c: 183 - 187

## Overview
Tests whether the current character in the Indonesian stemmer is a vowel according to the Indonesian vowel grouping definition.

## Definition


## Detailed Description
This function checks if the character at the current position in the stemmer environment is a vowel. It uses the Snowball framework's  function to test against the  character grouping, which defines Indonesian vowels in the ASCII range 97-117 (covering 'a' through 'u'). The function is used as a validation condition in the Indonesian stemming algorithm to ensure morphological transformations are applied correctly based on vowel/consonant patterns.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemmer environment and current position

## Dependencies
- Functions called/Symbols referenced:
  - in_grouping (Snowball framework function for character group testing)
  - g_vowel (character grouping array defining Indonesian vowels)
- Called from (representative examples):
  - Used as validation function in a_3 array for prefix 'meny' and 'peny' patterns

## Notes and Other Information
- Part of PostgreSQL's full-text search Snowball stemming implementation for Indonesian language
- Returns 1 if current character is a vowel, 0 if it's not a vowel
- The g_vowel grouping is defined as  representing vowel characters in Indonesian
- Generated automatically by Snowball compiler from Indonesian stemming rules
- Used primarily in conditional checks during prefix removal operations