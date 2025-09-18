# r_owned

## Location
src/backend/snowball/libstemmer/stem_UTF_8_hungarian.c: 709 - 738

## Overview
The r_owned function handles possessive suffix removal for the Hungarian stemming algorithm in the Snowball stemming library.

## Definition
```c
static int r_owned(struct SN_env * z)
```

## Detailed Description
The r_owned function is responsible for detecting and removing Hungarian possessive suffixes during the stemming process. It operates by:

1. Setting the ket position to the current cursor position
2. Checking if the character before the cursor is 'i' (ASCII 105) or 'é' (ASCII 233)
3. Using find_among_b to match against a set of 12 possessive suffix patterns (a_9 array)
4. Ensuring the match occurs within the R1 region
5. Performing appropriate transformations based on the matched pattern:
   - Case 1: Deletes the matched suffix
   - Case 2: Replaces with string s_8
   - Case 3: Replaces with string s_9

The function ensures that possessive suffix removal only occurs in appropriate morphological contexts by requiring matches to be within the R1 region and by checking for specific ending characters typical of Hungarian possessive forms.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing the word being stemmed, cursor positions, and other stemming state

## Dependencies
- Functions called/Symbols referenced:
  - r_R1 (region boundary test function)
  - find_among_b (backward pattern matching function)
  - slice_from_s (string replacement function)
  - slice_del (deletion function)
- Called from (representative examples):
  - hungarian_ISO_8859_2_stem
  - hungarian_UTF_8_stem

## Notes and Other Information
- This function is part of the Hungarian stemming algorithm implementation
- It specifically targets possessive forms by looking for 'i' or 'é' character patterns typical in Hungarian possessives
- The function returns 1 on successful application, 0 if no match is found, and negative values on error
- The pattern matching uses the a_9 array which contains 12 different possessive suffix patterns
- Region checking ensures morphologically appropriate suffix removal