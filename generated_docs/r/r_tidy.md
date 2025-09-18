# r_tidy

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_finnish.c:573-657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_finnish.c#L573-L657)

## Overview
The r_tidy function performs final cleanup operations during Finnish text stemming, removing redundant characters and normalizing vowel-consonant patterns to produce the final stemmed form.

## Definition
static int r_tidy(struct SN_env * z)

## Detailed Description
The r_tidy function is the final stage in the Finnish stemming algorithm that performs several cleanup operations to normalize the stemmed word. It operates within the R1 region (established by r_mark_regions) and performs the following operations:

1. **Long vowel normalization**: Uses the r_LONG function to detect long vowel patterns and removes redundant vowel characters
2. **Vowel-consonant pattern cleanup**: Removes specific vowel characters (AEI group: a, e, i, ä) that precede consonants in certain patterns
3. **Suffix cleanup**: Removes specific endings like 'oj' and 'uj' (masculine/neuter endings) and 'jo' patterns
4. **Final normalization**: Performs a final check to ensure vowel-consonant patterns are normalized, removing duplicate consonant patterns

The function uses backward processing (from end to beginning of the word) and employs various character group tests to identify valid patterns for removal.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the word being processed and stemming state information

## Dependencies
- Functions called/Symbols referenced:
  - [r_LONG](r_LONG.md)
  - [in_grouping_b](../i/in_grouping_b.md)
  - [slice_del](../s/slice_del.md)
  - [slice_to](../s/slice_to.md)
  - [eq_v_b](../e/eq_v_b.md)
- Called from (representative examples):
  - [finnish_ISO_8859_1_stem](../f/finnish_ISO_8859_1_stem.md)
  - [finnish_UTF_8_stem](../f/finnish_UTF_8_stem.md)

## Notes and Other Information
- This function is always called as the final step in the Finnish stemming process after all morphological endings have been removed
- Returns 1 on successful completion, 0 if no changes were made, or negative values on error
- The function operates only within the R1 region boundary to avoid over-stemming
- Uses character groups g_AEI, g_C, and g_V1 for pattern matching
- The cleanup operations are designed specifically for Finnish morphological patterns and may produce unexpected results on non-Finnish text