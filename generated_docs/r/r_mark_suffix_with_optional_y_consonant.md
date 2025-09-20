# r_mark_suffix_with_optional_y_consonant

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:580-613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L580-L613)

## Overview
This function handles the morphological analysis of Turkish suffixes that may optionally include a 'y' consonant, implementing consonant insertion rules for Turkish stemming.

## Definition

```c
}

static int r_mark_suffix_with_optional_y_consonant(struct SN_env * z)
```
## Detailed Description
The  function is designed to handle Turkish morphological patterns where the consonant 'y' may be optionally inserted in certain suffix contexts. This function follows the same structural pattern as the other optional consonant functions but specifically handles the 'y' consonant insertion pattern.

The 'y' insertion is particularly important in Turkish morphology as it serves as a buffer consonant to prevent vowel hiatus (consecutive vowels) in word formation. This commonly occurs when vowel-initial suffixes are attached to vowel-final stems.

The function implements the standard two-stage checking mechanism:
1. First, it checks if there is a 'y' at the current position preceded by a vowel - representing cases where the optional 'y' buffer consonant is present
2. If no 'y' is found, it validates the phonological constraints for cases where the 'y' is absent - ensuring no double 'y' consonants and that the preceding character is a vowel

This function is extensively used throughout the Turkish stemmer, reflecting the high frequency of 'y' insertion in Turkish morphological processes.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the text buffer, current position, and morphological analysis state

## Dependencies
- Functions called/Symbols referenced:
  - [in_grouping_b_U](../i/in_grouping_b_U.md) (called 2 times for vowel group checking)
  - [skip_b_utf8](../s/skip_b_utf8.md) (for UTF-8 character boundary handling)
  - g_vowel (vowel group definition)

- Called from (representative examples):
  - r_mark_yU (possessive suffix)
  - r_mark_yA (dative suffix)
  - [r_mark_ylA](r_mark_ylA.md) (instrumental suffix)
  - [r_mark_yUm](r_mark_yUm.md) (first person suffix)
  - [r_mark_yUz](r_mark_yUz.md) (second person plural suffix)
  - [r_mark_yDU](r_mark_yDU.md) (past tense suffix)
  - [r_mark_ysA](r_mark_ysA.md) (conditional suffix)
  - [r_mark_ymUs_](r_mark_ymUs_.md) (past tense first person plural)
  - [r_mark_yken](r_mark_yken.md) (temporal suffix)

## Notes and Other Information
- Returns 1 if the optional 'y' consonant pattern is valid, 0 otherwise
- Most frequently used among the optional consonant functions, reflecting the prevalence of 'y' insertion in Turkish
- The 'y' consonant serves as a hiatus breaker in Turkish phonology
- Structurally identical to other optional consonant functions but handles 'y' specifically
- Critical for processing vowel-initial suffixes that attach to vowel-final stems
- Used across a wide range of Turkish grammatical constructions (possessive, case marking, verbal inflection)
- Part of the comprehensive morphophonological rule system in PostgreSQL's Turkish Snowball stemmer