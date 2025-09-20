# r_append_U_to_stems_ending_with_d_or_g

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:1895-2004](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L1895-L2004)

## Overview
Appends appropriate vowel sounds ('ı', 'i', 'u', 'ü') to Turkish word stems ending with 'd' or 'g' consonants based on vowel harmony rules.

## Definition

```c
}

static int r_append_U_to_stems_ending_with_d_or_g(struct SN_env * z)
```
## Detailed Description
This function implements Turkish vowel harmony rules by appending the appropriate vowel to stems ending with 'd' or 'g' consonants. The function follows Turkish phonological rules where the choice of appended vowel depends on the preceding vowel context:

1. First checks if the stem ends with 'd' or 'g'
2. Analyzes the preceding vowel context to determine vowel harmony
3. Appends the appropriate vowel based on these rules:
   - After 'a' or 'ı': appends 'ı' 
   - After 'e' or 'i': appends 'i'
   - After 'o' or 'u': appends 'u'  
   - After 'ö' or 'ü': appends 'ü'

The function uses complex logic with multiple labels and backtracking to handle the various vowel harmony combinations correctly.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the string being processed and cursor positions

## Dependencies
- Functions called/Symbols referenced:
  - [out_grouping_b_U](../o/out_grouping_b_U.md) (Snowball function for backward vowel group testing)
  - [eq_s_b](../e/eq_s_b.md) (Snowball function for backward string equality testing)
  - [insert_s](../i/insert_s.md) (Snowball function for string insertion)
- Called from:
  - [r_postlude](r_postlude.md) (at src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:2052)

## Notes and Other Information
- This is a static function within the Turkish stemmer implementation
- Returns 1 on success, 0 if conditions are not met, or negative value on error
- Critical for maintaining Turkish vowel harmony in stemmed words
- Part of the post-processing phase in Turkish word stemming
- Generated automatically by Snowball 2.2.0 stemmer generator
- Uses UTF-8 encoded Turkish characters (ı, ö, ü) represented as byte sequences