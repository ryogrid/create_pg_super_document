# r_mark_yDU

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:861-871](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L861-L871)

## Overview
This function marks or identifies the Turkish suffix 'yDU' in the Snowball stemming algorithm, used for Turkish text stemming in PostgreSQL's full-text search functionality.

## Definition
```c
static int r_mark_yDU(struct SN_env * z)
```

## Detailed Description
The `r_mark_yDU` function is part of the Turkish stemming algorithm implementation in PostgreSQL's Snowball stemmer. It specifically handles the recognition and validation of the Turkish suffix 'yDU' which represents past tense forms in Turkish grammar. This function is more complex than other similar functions as it includes additional processing for optional 'y' consonant handling.

The function follows a three-step validation process:
1. First, it checks vowel harmony using `r_check_vowel_harmony`
2. Then it performs pattern matching using `find_among_b` with array `a_20` containing 32 different patterns
3. Finally, it processes optional 'y' consonant cases using `r_mark_suffix_with_optional_y_consonant`

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing the word being processed, cursor position, and other stemming state information

## Dependencies
- Functions called/Symbols referenced:
  - [r_check_vowel_harmony](r_check_vowel_harmony.md): Validates Turkish vowel harmony rules
  - [find_among_b](../f/find_among_b.md): Performs backward pattern matching from array a_20 (32 patterns)
  - [r_mark_suffix_with_optional_y_consonant](r_mark_suffix_with_optional_y_consonant.md): Handles optional 'y' consonant processing
- Called from:
  - [r_stem_nominal_verb_suffixes](r_stem_nominal_verb_suffixes.md): Used three times in the Turkish nominal and verb suffix stemming process (lines 913, 1003, and 1035)

## Notes and Other Information
- This function is part of the Turkish language-specific stemming rules
- The suffix 'yDU' is crucial for recognizing Turkish past tense forms
- Returns 1 on successful match, 0 on failure, following standard Snowball stemming conventions
- Uses the largest pattern array (32 patterns) among similar functions, indicating the high variability of 'yDU' suffix forms
- The additional `r_mark_suffix_with_optional_y_consonant` call shows the complexity of Turkish phonological rules
- Called multiple times in the stemming process, demonstrating its importance in Turkish morphological analysis
- Part of the comprehensive Turkish morphological analysis system in PostgreSQL's full-text search