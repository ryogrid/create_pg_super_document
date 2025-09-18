# r_remove_first_order_prefix

## Location
src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c: 194 - 273

## Overview
Removes first-order prefixes from Indonesian words including 'di', 'ke', 'me', 'mem', 'men', 'meng', 'meny', 'pem', 'pen', 'peng', 'peny', and 'ter' with appropriate morphological transformations.

## Definition


## Detailed Description
This function implements the removal of Indonesian first-order prefixes as part of the Snowball stemming algorithm. It searches for prefixes defined in the  array and performs different transformations based on the matched prefix type. The function handles complex morphological rules including consonant restoration (adding 's' or 'p' characters), vowel-based conditional transformations, and proper morphological type tracking through  and word length adjustment through . Each case represents different morphological patterns: simple deletion, consonant restoration, or conditional vowel-based transformations.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemmer environment with the word being processed

## Dependencies
- Functions called/Symbols referenced:
  - find_among (Snowball framework function for forward string matching)
  - slice_del (Snowball framework function for string deletion) 
  - slice_from_s (Snowball framework function for string replacement)
  - in_grouping (Snowball framework function for character group testing)
  - a_3 (array defining 12 first-order prefixes with their patterns)
  - s_1, s_2, s_3, s_4 (string constants for consonant restoration)
  - g_vowel (character grouping for vowel testing)
- Called from (representative examples):
  - indonesian_ISO_8859_1_stem (src/backend/snowball/libstemmer/stem_ISO_8859_1_indonesian.c:352)
  - indonesian_UTF_8_stem (src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:352)

## Notes and Other Information
- Part of PostgreSQL's full-text search Snowball stemming implementation for Indonesian language
- Returns 1 on successful prefix removal, 0 if no applicable prefix found, or negative value on error
- Uses morphological type codes: I[0]=1 for certain prefixes, I[0]=3 for others
- Includes complex vowel-based conditional logic for 'mem' and 'pem' prefixes (cases 5 and 6)
- Pre-checks for ending characters (105='i', 101='e') for performance optimization
- Generated automatically by Snowball compiler from Indonesian stemming rules