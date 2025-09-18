# r_mark_DUr

## Location
src/backend/snowball/libstemmer/stem_UTF_8_turkish.c: 846 - 854

## Overview
This function marks or identifies the Turkish suffix 'DUr' in the Snowball stemming algorithm, used for Turkish text stemming in PostgreSQL's full-text search functionality.

## Definition
```c
static int r_mark_DUr(struct SN_env * z)
```

## Detailed Description
The `r_mark_DUr` function is part of the Turkish stemming algorithm implementation in PostgreSQL's Snowball stemmer. It specifically handles the recognition and validation of the Turkish suffix 'DUr' which appears in various forms due to vowel harmony rules in Turkish. This suffix is commonly used in Turkish grammar for present tense third person singular forms and other grammatical constructions.

The function follows a two-step validation process:
1. First, it checks vowel harmony using `r_check_vowel_harmony`
2. Then it performs boundary checking and pattern matching using `find_among_b` with array `a_18` containing 8 different patterns

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing the word being processed, cursor position, and other stemming state information

## Dependencies
- Functions called/Symbols referenced:
  - r_check_vowel_harmony: Validates Turkish vowel harmony rules
  - find_among_b: Performs backward pattern matching from array a_18 (8 patterns)
- Called from:
  - r_stem_nominal_verb_suffixes: Used twice in the Turkish nominal and verb suffix stemming process (lines 996 and 1095)

## Notes and Other Information
- This function is part of the Turkish language-specific stemming rules
- The suffix 'DUr' is a significant Turkish grammatical suffix requiring special vowel harmony handling
- Returns 1 on successful match, 0 on failure, following standard Snowball stemming conventions
- The function checks that there are at least 2 characters before the current position and that the character before the cursor is 'r' (ASCII 114)
- Uses a larger pattern array (8 patterns) compared to similar functions, indicating the complexity of 'DUr' suffix variations
- Called multiple times in the stemming process, showing its importance in Turkish morphological analysis