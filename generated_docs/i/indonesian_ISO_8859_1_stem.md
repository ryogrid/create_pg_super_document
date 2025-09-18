# indonesian_ISO_8859_1_stem

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_indonesian.c:313-403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_indonesian.c#L313-L403)

## Overview
This function implements the complete Indonesian word stemming algorithm for ISO-8859-1 encoded text, applying a sequence of morphological transformations to reduce words to their base forms.

## Definition


## Detailed Description
The function performs Indonesian stemming through a multi-stage process that follows the Snowball stemming algorithm for Indonesian. The stemming process includes:

1. **Vowel counting**: Counts vowels in the word to determine if stemming should proceed (requires > 2 vowels)
2. **Suffix removal**: Removes particles ('kah', 'lah', 'pun') and possessive pronouns ('nya', 'ku', 'mu') from the end of words
3. **Prefix removal**: Applies a complex prefix removal strategy with two possible paths:
   - Path A: Remove first-order prefixes, then optionally remove suffixes and second-order prefixes
   - Path B: If first-order prefix removal fails, try second-order prefix removal followed by optional suffix removal

The algorithm maintains strict vowel count checks (> 2 vowels) before each major transformation to prevent over-stemming of short words. The stemming environment tracks the current position in the word and maintains integer variables for vowel counts and processing state.

## Parameters / Member Variables
- : Pointer to the Snowball stemming environment (SN_env) containing the word to be stemmed and processing state

## Dependencies
- Functions called/Symbols referenced:
  - [out_grouping](../o/out_grouping.md) (for vowel detection)
  - [r_remove_particle](../r/r_remove_particle.md)
  - [r_remove_possessive_pronoun](../r/r_remove_possessive_pronoun.md)  
  - [r_remove_first_order_prefix](../r/r_remove_first_order_prefix.md)
  - [r_remove_second_order_prefix](../r/r_remove_second_order_prefix.md)
  - [r_remove_suffix](../r/r_remove_suffix.md)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This is the main entry point for Indonesian stemming in the Snowball library
- Uses ISO-8859-1 character encoding for Indonesian text processing
- Returns 1 on successful completion, 0 if vowel count is insufficient, or negative values on error
- The algorithm implements the Indonesian language-specific rules developed for the Snowball stemming project
- The function modifies the input word in-place within the stemming environment