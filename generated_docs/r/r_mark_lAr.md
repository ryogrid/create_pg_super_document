# r_mark_lAr

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:828-836](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L828-L836)

## Overview
A static function in the Turkish stemmer that identifies and marks the suffix "lAr" (meaning "they" or plural marker) and its variations, used in Turkish morphological analysis for both nominal and verbal conjugations.

## Definition
```c
static int r_mark_lAr(struct SN_env * z)
```

## Detailed Description
This function is part of the Turkish language stemmer in PostgreSQL's Snowball implementation. It identifies the Turkish suffix "lAr" and its phonetic variation "ler" which serve as plural markers in Turkish nouns and third person plural markers in verbs. The "A" in "lAr" represents vowel harmony - it becomes either "a" or "e" depending on the vowels in the root word. The function performs vowel harmony checking before attempting to match the suffix pattern.

The function works by:
1. First checking vowel harmony using `r_check_vowel_harmony()`
2. Checking if the current character is 'r' (ASCII 114)
3. Ensuring there are at least 2 characters before the current position (z->c - 2 <= z->lb)
4. Using `find_among_b()` to match against the predefined suffix array `a_16`

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the stemming environment, including the string being processed, cursor position, and other state information

## Dependencies
- Functions called/Symbols referenced:
  - [r_check_vowel_harmony](r_check_vowel_harmony.md)
  - [find_among_b](../f/find_among_b.md)
  - a_16 (static array containing suffix patterns: "lar", "ler")
- Called from (representative examples):
  - [r_stem_nominal_verb_suffixes](r_stem_nominal_verb_suffixes.md) (at lines 948, 985, 1112)
  - [r_stem_suffix_chain_before_ki](r_stem_suffix_chain_before_ki.md) (at lines 1175, 1204, 1270, 1325)
  - [r_stem_noun_suffixes](r_stem_noun_suffixes.md) (at multiple lines: 1357, 1420, 1439, 1496, 1549, 1598, 1616, 1669, 1705, 1793, 1803, 1844)

## Notes and Other Information
- The `a_16` array contains only 2 suffix variations: "lar" (back vowel harmony) and "ler" (front vowel harmony)
- This is one of the most frequently used functions in the Turkish stemmer, appearing in multiple stemming contexts
- The "lAr" suffix is fundamental in Turkish grammar, used for both noun pluralization and third person plural verb conjugation
- Unlike other suffix marking functions, this one doesn't require optional consonant handling as 'l' and 'r' are always present
- The function returns 1 on successful match, 0 on failure, and propagates negative return values from called functions
- This is part of the Turkish morphological analysis system used for full-text search indexing in PostgreSQL

## Simplified Source

```c
static int r_mark_lAr(struct SN_env * z) {
    // Check vowel harmony compliance
    int ret = r_check_vowel_harmony(z);
    if (ret <= 0) return ret;

    // Verify position and current character is 'r' (ASCII 114)
    if (z->c - 2 <= z->lb || z->p[z->c - 1] != 114) return 0;

    // Match against "lAr" suffix patterns (lar, ler)
    if (!find_among_b(z, a_16, 2)) return 0;

    return 1; // Success
}
```