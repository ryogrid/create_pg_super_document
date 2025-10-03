# r_mark_nUz

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:837-845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L837-L845)

## Overview
This function marks or identifies the Turkish suffix 'nUz' in the Snowball stemming algorithm, used for Turkish text stemming in PostgreSQL's full-text search functionality.

## Definition

```c
}

static int r_mark_nUz(struct SN_env * z)
```
## Detailed Description
The  function is part of the Turkish stemming algorithm implementation in PostgreSQL's Snowball stemmer. It specifically handles the recognition and validation of the Turkish suffix 'nUz' which appears in various forms due to vowel harmony rules in Turkish. The function performs vowel harmony checking and uses backward pattern matching to identify the suffix at the end of a word.

The function follows a two-step validation process:
1. First, it checks vowel harmony using 
2. Then it performs boundary checking and pattern matching using  with array 

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the word being processed, cursor position, and other stemming state information
## Dependencies
- Functions called/Symbols referenced:
  - [r_check_vowel_harmony](r_check_vowel_harmony.md): Validates Turkish vowel harmony rules
  - [find_among_b](../f/find_among_b.md): Performs backward pattern matching from array a_17 (4 patterns)
- Called from:
  - [r_stem_nominal_verb_suffixes](r_stem_nominal_verb_suffixes.md): Part of the Turkish nominal and verb suffix stemming process

## Notes and Other Information
- This function is part of the Turkish language-specific stemming rules
- The suffix 'nUz' is a common Turkish grammatical suffix that needs special handling due to vowel harmony
- Returns 1 on successful match, 0 on failure, following standard Snowball stemming conventions
- The function checks that there are at least 2 characters before the current position and that the character before the cursor is 'z' (ASCII 122)
- Part of the comprehensive Turkish stemming algorithm that handles the complex morphology of the Turkish language