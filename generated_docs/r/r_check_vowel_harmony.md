# r_check_vowel_harmony

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:459-511](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L459-L511)

## Overview
This function implements vowel harmony checking for Turkish text processing in the PostgreSQL Snowball stemmer, ensuring that vowels in a word follow Turkish phonological rules.

## Definition


## Detailed Description
The  function is a critical component of the Turkish stemmer that validates whether a word follows Turkish vowel harmony rules. Turkish vowel harmony requires that vowels within a word must harmonize according to frontness/backness and roundedness features. The function performs backward scanning from the current position to check if the preceding vowels conform to these phonological constraints.

The function implements a complex decision tree that:
1. Identifies the last vowel before the current position
2. Determines the appropriate vowel group based on that vowel ('a', 'e', 'ı', 'i', 'o', 'ö', 'u', 'ü')
3. Validates that any preceding vowels belong to the harmonically compatible group
4. Uses multiple vowel groupings (g_vowel1 through g_vowel6) to enforce different harmony patterns

This is essential for correct Turkish morphological analysis, as suffix attachment and word formation in Turkish must respect vowel harmony constraints.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the text buffer, current position, and other stemming state information

## Dependencies
- Functions called/Symbols referenced:
  - [out_grouping_b_U](../o/out_grouping_b_U.md) (called 12 times for vowel group checking)
  - [eq_s_b](../e/eq_s_b.md) (called 3 times for string equality checking)
  - Various vowel groups: g_vowel, g_vowel1, g_vowel2, g_vowel3, g_vowel4, g_vowel5, g_vowel6
  - [String](../S/String.md) constants: s_0, s_1, s_2

- Called from (representative examples):
  - [r_mark_sU](r_mark_sU.md)
  - [r_mark_yU](r_mark_yU.md)
  - [r_mark_nU](r_mark_nU.md)
  - [r_mark_nUn](r_mark_nUn.md)
  - [r_mark_yA](r_mark_yA.md)
  - [r_mark_nA](r_mark_nA.md)
  - [r_mark_DA](r_mark_DA.md)
  - [r_mark_ndA](r_mark_ndA.md)
  - [r_mark_DAn](r_mark_DAn.md)
  - [r_mark_ndAn](r_mark_ndAn.md)
  - [r_mark_ylA](r_mark_ylA.md)
  - [r_mark_ncA](r_mark_ncA.md)
  - [r_mark_yUm](r_mark_yUm.md)
  - [r_mark_sUn](r_mark_sUn.md)
  - [r_mark_yUz](r_mark_yUz.md)
  - [r_mark_lAr](r_mark_lAr.md)
  - [r_mark_nUz](r_mark_nUz.md)
  - [r_mark_DUr](r_mark_DUr.md)
  - [r_mark_yDU](r_mark_yDU.md)
  - [r_mark_ymUs_](r_mark_ymUs_.md)

## Notes and Other Information
- Returns 1 if vowel harmony is satisfied, 0 if violated
- Uses backward scanning (suffix to root direction) which is typical for agglutinative languages like Turkish
- The function is heavily used by suffix marking functions, indicating its central role in Turkish morphological processing
- The complex branching structure reflects the intricate nature of Turkish vowel harmony rules
- Part of the automatically generated Snowball stemmer code for Turkish language processing within PostgreSQL's full-text search capabilities