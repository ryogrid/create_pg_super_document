# r_remove_suffix

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:171-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c#L171-L182)

## Overview
Removes suffixes ('i', 'an', 'kan') from Indonesian words as part of the Snowball stemming algorithm for Indonesian language text processing.

## Definition


## Detailed Description
This function implements suffix removal logic for Indonesian word stemming. It searches backwards from the current position for specific suffixes defined in the  array ('i', 'an', 'kan') and removes them if found. The function uses the Snowball stemmer framework's string matching capabilities and includes validation through helper functions (r_SUFFIX_I_OK, r_SUFFIX_AN_OK, r_SUFFIX_KAN_OK) to ensure the suffix removal is linguistically appropriate. After successful removal, it decrements the morphological analysis counter .

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemmer environment with the word being processed

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (Snowball framework function for backward string matching)
  - [slice_del](../s/slice_del.md) (Snowball framework function for string deletion)
  - a_2 (array defining suffixes 'i', 'an', 'kan' with their validation functions)
  - [r_SUFFIX_I_OK](r_SUFFIX_I_OK.md), r_SUFFIX_AN_OK, r_SUFFIX_KAN_OK (validation functions)
- Called from (representative examples):
  - [indonesian_ISO_8859_1_stem](../i/indonesian_ISO_8859_1_stem.md) (src/backend/snowball/libstemmer/stem_ISO_8859_1_indonesian.c:361, 391)
  - [indonesian_UTF_8_stem](../i/indonesian_UTF_8_stem.md) (src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:361, 391)

## Notes and Other Information
- Part of PostgreSQL's full-text search Snowball stemming implementation for Indonesian language
- Returns 1 on successful suffix removal, 0 if no applicable suffix found, or negative value on error
- Checks for specific ending characters (105='i', 110='n') before attempting pattern matching for performance
- Generated automatically by Snowball compiler from Indonesian stemming rules