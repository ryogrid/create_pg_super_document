# r_Step_5b

## Location
src/backend/snowball/libstemmer/stem_UTF_8_porter.c: 548 - 563

## Overview
The r_Step_5b function implements Step 5b of the Porter stemming algorithm, which removes double 'l' characters at the end of words when they are in the R2 region.

## Definition


## Detailed Description
This function performs the final step (5b) of the Porter stemming algorithm. It specifically handles the removal of double 'l' characters at the end of words, but only when the second 'l' falls within the R2 region (the advanced morphological boundary). The function follows the Porter algorithm specification by:

1. Setting the ket (end marker) to the current cursor position
2. Checking if the current character is 'l'
3. Moving the cursor back and setting bra (beginning marker)
4. Verifying that the position is within the R2 region
5. Checking for another 'l' character
6. If conditions are met, deleting the double 'l' suffix

This step ensures that words ending in double 'l' are properly stemmed according to English morphological rules, such as converting "mill" to "mill" (no change) but handling cases where the double 'l' is part of a derivational suffix.

## Parameters / Member Variables
- : Pointer to the SN_env structure containing the stemming environment, including the word being processed, cursor positions, and region boundaries

## Dependencies
- Functions called/Symbols referenced:
  - [r_R2](r_R2.md) (tests if cursor position is within R2 region)
  - [slice_del](../s/slice_del.md) (deletes characters between bra and ket markers)
- Called from (representative examples):
  - [porter_ISO_8859_1_stem](../p/porter_ISO_8859_1_stem.md)
  - [porter_UTF_8_stem](../p/porter_UTF_8_stem.md)

## Notes and Other Information
- Returns 1 on successful application of the rule, 0 if the rule doesn't apply
- This is the final step in the Porter stemming algorithm sequence
- The function is part of the Snowball stemming library integrated into PostgreSQL
- The double 'l' removal only occurs when both 'l' characters are present and the second falls within the R2 region
- File location: src/backend/snowball/libstemmer/stem_ISO_8859_1_porter.c:546-561