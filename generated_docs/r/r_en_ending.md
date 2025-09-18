# r_en_ending

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_dutch.c:368-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_dutch.c#L368-L390)

## Overview
r_en_ending is a specialized function in the Dutch Snowball stemming algorithm that handles the removal of 'en' suffix endings from words with specific vowel pattern and exclusion requirements.

## Definition


## Detailed Description
The r_en_ending function implements a complex rule for Dutch stemming that removes 'en' suffixes when specific morphological conditions are met. The function performs a multi-step validation process:

1. **Region Validation**: Uses r_R1 to ensure the current position is within the R1 region
2. **Vowel Pattern Check**: Uses out_grouping_b to verify that the character preceding the 'en' suffix is NOT a vowel (group g_v, range 97-232)
3. **Exclusion Check**: Uses eq_s_b to check for a specific 3-character exclusion pattern (s_10) that would prevent 'en' removal
4. **Conditional Removal**: If the exclusion pattern is found, the function returns 0 (no action); otherwise, it proceeds
5. **Suffix Deletion**: Removes the 'en' suffix using slice_del
6. **Consonant Cleanup**: Calls r_undouble to handle any doubled consonants that may result from the suffix removal

This function ensures that 'en' suffixes are only removed when they follow consonants and don't match specific exclusion patterns, preventing incorrect stemming in Dutch morphology.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemming environment with cursor positions, boundaries, and character data

## Dependencies
- Functions called/Symbols referenced:
  - [r_R1](r_R1.md): Validates that the current position is within the R1 region
  - [out_grouping_b](../o/out_grouping_b.md): Checks if character is NOT in specified vowel group
  - [eq_s_b](../e/eq_s_b.md): Checks for specific string pattern match (3-character exclusion pattern s_10)
  - [slice_del](../s/slice_del.md): Removes character sequence from the word
  - [r_undouble](r_undouble.md): Removes doubled consonants after suffix removal
- Called from (representative examples):
  - [r_standard_suffix](r_standard_suffix.md): Dutch standard suffix processing (multiple locations)

## Notes and Other Information
- The function includes a specific exclusion mechanism using s_10 pattern matching to prevent incorrect 'en' suffix removal
- The vowel group check (g_v, 97-232) covers the Dutch vowel character set including accented characters
- Uses the goto/label mechanism (lab0) for control flow in the exclusion pattern check
- The integration with r_undouble ensures proper consonant doubling cleanup after suffix removal
- Only called from r_standard_suffix, indicating its role as a specialized sub-operation in Dutch stemming
- Available in both ISO-8859-1 and UTF-8 variants for different character encodings
- The m1 and m2 variables are used for position tracking with explicit void casting to suppress unused variable warnings