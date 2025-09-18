# r_remove_second_order_prefix

## Location
src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c: 274 - 312

## Overview
Removes second-order prefixes from Indonesian words including 'be', 'belajar', 'ber', 'pe', 'pelajar', and 'per' with appropriate morphological transformations and root word restoration.

## Definition


## Detailed Description
This function implements the removal of Indonesian second-order prefixes as part of the Snowball stemming algorithm. It searches for prefixes defined in the  array and performs different transformations based on the matched prefix type. The function handles complex morphological rules including complete deletion of simple prefixes, restoration of root words by replacing compound prefixes with their underlying roots (e.g., 'belajar' → 'ajar', 'pelajar' → 'ajar'), and proper morphological type tracking through  settings. Second-order prefixes are processed after first-order prefixes have been removed.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemmer environment with the word being processed

## Dependencies
- Functions called/Symbols referenced:
  - [find_among](../f/find_among.md) (Snowball framework function for forward string matching)
  - [slice_del](../s/slice_del.md) (Snowball framework function for string deletion)
  - [slice_from_s](../s/slice_from_s.md) (Snowball framework function for string replacement)
  - a_4 (array defining 6 second-order prefixes: 'be', 'belajar', 'ber', 'pe', 'pelajar', 'per')
  - s_5, s_6 (string constants containing 'ajar' for root word restoration)
  - [r_KER](r_KER.md) (validation function for 'be' prefix requiring consonant+'er' pattern)
- Called from (representative examples):
  - [indonesian_ISO_8859_1_stem](../i/indonesian_ISO_8859_1_stem.md) (src/backend/snowball/libstemmer/stem_ISO_8859_1_indonesian.c:369, 382)
  - [indonesian_UTF_8_stem](../i/indonesian_UTF_8_stem.md) (src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:369, 382)

## Notes and Other Information
- Part of PostgreSQL's full-text search Snowball stemming implementation for Indonesian language
- Returns 1 on successful prefix removal, 0 if no applicable prefix found, or negative value on error
- Uses morphological type codes: I[0]=2 for some prefixes, I[0]=4 for others
- Pre-checks for ending character (101='e') for performance optimization since all second-order prefixes end with 'e'
- Cases 2 and 4 restore 'ajar' root from 'belajar' and 'pelajar' compound prefixes respectively
- Generated automatically by Snowball compiler from Indonesian stemming rules