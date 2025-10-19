# r_mark_nU

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:683-690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L683-L690)

## Overview
A static function in the Turkish stemmer that identifies and marks suffixes containing the Turkish vowel 'U' preceded by 'n' consonant with vowel harmony validation.

## Definition

```c
}

static int r_mark_nU(struct SN_env * z)
```
## Detailed Description
This function is part of the Turkish language stemming implementation that specifically handles suffixes containing the vowel 'U' (representing both 'u' and 'ü' in Turkish vowel harmony) preceded by the consonant 'n'. The function operates in two main steps: first, it validates vowel harmony to ensure the suffix is phonologically compatible with the word stem according to Turkish linguistic rules; second, it uses backwards pattern matching against an array of 4 predefined 'nU' suffix patterns () that account for different vowel harmony variants. This function is more specialized than other mark functions as it targets a specific consonant-vowel combination that appears in certain Turkish suffixes.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the word being processed, current position markers, and other stemming state information
## Dependencies
- Functions called/Symbols referenced:
  - [r_check_vowel_harmony](r_check_vowel_harmony.md) (validates Turkish vowel harmony rules)
  - [find_among_b](../f/find_among_b.md) (Snowball library function for backwards pattern matching)
  - a_2 (array of 4 'nU' suffix patterns with vowel harmony variants)
- Called from (representative examples):
  - [r_stem_noun_suffixes](r_stem_noun_suffixes.md)

## Notes and Other Information
- Returns 1 on successful 'nU' suffix identification and marking, 0 on failure
- Uses a relatively small pattern array (4 patterns) compared to other suffix marking functions
- The 'n' consonant is significant in Turkish grammar as it appears in various grammatical suffixes
- Vowel harmony validation is essential as Turkish suffix vowels must harmonize with stem vowels
- Less frequently used compared to more general mark functions, indicating it targets specific suffix types
- Part of the comprehensive Turkish morphological analysis system for accurate text processing and search

## Simplified Source

```c
static int r_mark_nU(struct SN_env * z) {
    // Validate vowel harmony first
    int harmony_check = r_check_vowel_harmony(z);
    if (harmony_check <= 0) return harmony_check;

    // Match against 4 'nU' suffix patterns
    if (!(find_among_b(z, a_2, 4)))
        return 0;

    return 1;  // Successfully found nU suffix pattern
}
```