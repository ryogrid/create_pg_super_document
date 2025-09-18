# r_vowel_suffix

## Location
src/backend/snowball/libstemmer/stem_UTF_8_romanian.c: 899 - 911

## Overview
The r_vowel_suffix function handles the removal of vowel suffixes and specific consonant-vowel combinations at word endings in Italian and Romanian Snowball stemming algorithms.

## Definition
```c
static int r_vowel_suffix(struct SN_env * z)
```

## Detailed Description
This function performs two distinct suffix removal operations:

1. **Vowel Suffix Removal**: Identifies and removes vowel suffixes (AEIO group) that occur within the RV region. If the suffix ends with 'i', it removes the 'i' as well, provided it's also in the RV region.

2. **Consonant-H Removal**: Removes 'h' characters that are preceded by consonants from the CG group (c, g) within the RV region.

The function uses backtracking mechanisms (m1, m2 markers) to restore the cursor position if conditions aren't met, ensuring safe exploration of potential suffix matches.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing the word being processed, cursor positions, and character groups

## Dependencies
- Functions called/Symbols referenced:
  - in_grouping_b (checks if character belongs to specified group, moving backwards)
  - r_RV (tests if current position is within the RV morphological region)  
  - slice_del (removes text between bra and ket markers)
- Called from (representative examples):
  - italian_ISO_8859_1_stem
  - italian_UTF_8_stem
  - romanian_ISO_8859_2_stem
  - romanian_UTF_8_stem

## Notes and Other Information
- This function is part of the suffix removal phase in Italian and Romanian stemming
- Uses the standard Snowball pattern of setting ket/bra markers to define deletion boundaries
- The function processes suffixes in reverse order (backwards through the string)
- Returns 1 on successful completion, negative values on errors
- The RV region check ensures suffixes are only removed from morphologically significant parts of the word