# r_more_than_one_syllable_word

## Location
src/backend/snowball/libstemmer/stem_UTF_8_turkish.c: 2016 - 2038

## Overview
Checks if a Turkish word contains more than one syllable by counting vowel groups, which is essential for determining if stemming operations should be applied.

## Definition


## Detailed Description
This function implements syllable counting for Turkish words by detecting vowel groups. In Turkish, each syllable typically contains one vowel, so counting vowels effectively counts syllables. The function:

1. Attempts to find at least 2 vowel groups in the word
2. Uses a loop with counter  starting at 2
3. Moves the cursor forward through non-vowel characters using 
4. Decrements the counter for each vowel group found
5. Returns 1 if at least 2 vowel groups are found (i.e., more than one syllable)
6. Returns 0 if fewer than 2 vowel groups are found (monosyllabic)

This check is crucial because many Turkish stemming operations should only be applied to multi-syllabic words to avoid over-stemming short words.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the string being processed and cursor positions

## Dependencies
- Functions called/Symbols referenced:
  - out_grouping_U (Snowball function for forward vowel group testing using g_vowel character group)
- Called from:
  - turkish_UTF_8_stem (at src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:2068)

## Notes and Other Information
- This is a static function within the Turkish stemmer implementation
- Returns 1 if the word has more than one syllable, 0 otherwise
- Essential guard condition to prevent inappropriate stemming of monosyllabic words
- Uses the g_vowel character group that defines Turkish vowels (a, e, ı, i, o, ö, u, ü)
- Part of the initial validation phase in Turkish word stemming
- Generated automatically by Snowball 2.2.0 stemmer generator