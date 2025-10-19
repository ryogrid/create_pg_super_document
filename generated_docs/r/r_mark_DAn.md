# r_mark_DAn

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:742-750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L742-L750)

## Overview
A static function in the Turkish snowball stemmer that checks for the accusative case suffix "DAn" while ensuring vowel harmony compliance.

## Definition

```c
}

static int r_mark_DAn(struct SN_env * z)
```
## Detailed Description
This function is part of the Turkish language stemmer implementation in PostgreSQL's snowball library. It specifically identifies and validates the accusative case suffix "DAn" (and its vowel harmony variants like "Dan", "Den", "Tan", "Ten") in Turkish words. The function performs a two-step validation: first checking vowel harmony rules, then verifying the suffix pattern against a predefined set of suffixes.

The function operates by:
1. Calling r_check_vowel_harmony() to ensure the suffix follows Turkish vowel harmony rules
2. Checking that the current position has at least 2 characters before the left boundary
3. Verifying the last character is 'n' (ASCII 110)
4. Using find_among_b() to match against suffix patterns in array a_8

## Parameters / Member Variables
- `*z`: Pointer to SN_env structure containing the stemming environment, including the word being processed, current position, and boundaries
## Dependencies
- Functions called/Symbols referenced:
  - [r_check_vowel_harmony](r_check_vowel_harmony.md)
  - [find_among_b](../f/find_among_b.md) (with array a_8)
- Called from (representative examples):
  - [r_stem_noun_suffixes](r_stem_noun_suffixes.md)

## Notes and Other Information
- Returns 1 on successful match, 0 on failure
- Part of the Turkish noun suffix stemming process
- The "DAn" suffix in Turkish indicates the ablative case ("from" in English)
- The array a_8 contains 4 different suffix variants to accommodate vowel harmony
- This function is automatically generated code from snowball stemming algorithms

## Simplified Source

```c
static int r_mark_DAn(struct SN_env * z) {
    // Check vowel harmony first
    int harmony_check = r_check_vowel_harmony(z);
    if (harmony_check <= 0) return harmony_check;

    // Ensure we have space for 3-char pattern and last character is 'n'
    if (z->c - 2 <= z->lb || z->p[z->c - 1] != 110)
        return 0;

    // Match against 4 'DAn' suffix patterns (dan, tan, den, ten)
    if (!(find_among_b(z, a_8, 4)))
        return 0;

    return 1;  // Successfully found DAn suffix pattern
}
```