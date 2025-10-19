# r_mark_yUz

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:810-821](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L810-L821)

## Overview
A static function in the Turkish stemmer that identifies and marks the suffix "yUz" (meaning "we are" in Turkish) and its variations, used in Turkish verb conjugation processing.

## Definition
```c
static int r_mark_yUz(struct SN_env * z)
```

## Detailed Description
This function is part of the Turkish language stemmer in PostgreSQL's Snowball implementation. It identifies the Turkish suffix "yUz" and its phonetic variations ("iz", "uz", "ız", "üz") which represent the first person plural present tense form "we are" in Turkish verbs. The function performs vowel harmony checking before attempting to match the suffix pattern, ensuring linguistic correctness according to Turkish grammar rules.

The function works by:
1. First checking vowel harmony using `r_check_vowel_harmony()`
2. Checking if the current character is 'z' (ASCII 122)
3. Using `find_among_b()` to match against the predefined suffix array `a_14`
4. Finally calling `r_mark_suffix_with_optional_y_consonant()` to handle the optional 'y' consonant insertion

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the stemming environment, including the string being processed, cursor position, and other state information

## Dependencies
- Functions called/Symbols referenced:
  - [r_check_vowel_harmony](r_check_vowel_harmony.md)
  - [find_among_b](../f/find_among_b.md)
  - [r_mark_suffix_with_optional_y_consonant](r_mark_suffix_with_optional_y_consonant.md)
  - a_14 (static array containing suffix patterns: "iz", "uz", "ız", "üz")
- Called from (representative examples):
  - [r_stem_nominal_verb_suffixes](r_stem_nominal_verb_suffixes.md) (at lines 969, 1059, 1133)

## Notes and Other Information
- The `a_14` array contains 4 suffix variations: "iz", "uz", "ız" (with dotless ı), and "üz"
- These suffixes follow Turkish vowel harmony rules for first person plural conjugation
- Similar to r_mark_yUm, this function includes the optional 'y' consonant handling mechanism
- The function checks the character 'z' at position (z->c - 1) to ensure it's looking at the right suffix ending
- The function returns 1 on successful match, 0 on failure, and propagates negative return values from called functions
- This is part of the Turkish morphological analysis system used for full-text search indexing in PostgreSQL

## Simplified Source

```c
static int r_mark_yUz(struct SN_env * z) {
    // Check vowel harmony compliance
    int ret = r_check_vowel_harmony(z);
    if (ret <= 0) return ret;

    // Verify current character is 'z' (ASCII 122)
    if (z->c - 1 <= z->lb || z->p[z->c - 1] != 122) return 0;

    // Match against "yUz" suffix patterns (iz, uz, ız, üz)
    if (!find_among_b(z, a_14, 4)) return 0;

    // Handle optional 'y' consonant insertion
    ret = r_mark_suffix_with_optional_y_consonant(z);
    if (ret <= 0) return ret;

    return 1; // Success
}
```