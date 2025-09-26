# r_mark_yU

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:672-682](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L672-L682)

## Overview
A static function in the Turkish stemmer that identifies and marks suffixes containing the Turkish vowel 'U' preceded by optional 'y' consonant with proper vowel harmony validation.

## Definition

```c
}

static int r_mark_yU(struct SN_env * z)
```
## Detailed Description
This function is part of the Turkish language stemming implementation that handles suffixes containing the vowel 'U' (representing both 'u' and 'ü' in Turkish vowel harmony) with an optional preceding 'y' consonant. The function follows a three-step process: first, it validates vowel harmony to ensure the suffix is phonologically compatible with the word stem according to Turkish rules; second, it checks if the current character belongs to the U vowel group (specifically checking for characters 105 and 305 which represent 'ı' and 'İ' in UTF-8); finally, it marks the suffix while handling the optional 'y' consonant that commonly appears before U vowels in Turkish morphology.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the word being processed, current position markers, and other stemming state information

## Dependencies
- Functions called/Symbols referenced:
  - r_check_vowel_harmony (validates Turkish vowel harmony rules)
  - in_grouping_b_U (checks if character belongs to U vowel group, backward direction)
  - r_mark_suffix_with_optional_y_consonant (handles optional 'y' consonant in suffixes)
  - g_U (vowel group definition for U vowels)
- Called from (representative examples):
  - r_stem_noun_suffixes

## Notes and Other Information
- Returns 1 on successful suffix identification and marking, 0 on failure
- The character range 105-305 corresponds to specific Turkish characters ('ı' and 'İ') in UTF-8 encoding
- The 'y' consonant is often inserted as a buffer between vowels in Turkish to maintain phonetic flow
- Vowel harmony checking is crucial as Turkish suffixes must harmonize with stem vowels
- Less frequently used compared to other mark functions, primarily called from noun suffix processing
- Part of the comprehensive Turkish morphological analysis system that handles complex suffix variations