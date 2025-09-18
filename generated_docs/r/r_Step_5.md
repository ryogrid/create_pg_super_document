# r_Step_5

## Location
src/backend/snowball/libstemmer/stem_UTF_8_english.c: 820 - 868

## Overview
Implements Step 5 of the Porter stemming algorithm, removing or handling final "e" and "l" characters based on specific morphological conditions.

## Definition
```c
static int r_Step_5(struct SN_env * z)
```

## Detailed Description
This function performs Step 5 of the Porter stemming algorithm for English words, which is the final step that deals with removing final "e" and "l" characters under specific conditions. The function handles two cases:

1. **Final "e" removal**: The "e" is deleted if it occurs in R2, or if it occurs in R1 but is not preceded by a short syllable (as determined by r_shortv).

2. **Final "l" removal**: The "l" is deleted if it occurs in R2 and is preceded by another "l".

This step is crucial for proper stemming as it prevents over-aggressive removal of important word endings while still normalizing common morphological variations.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the Snowball environment with the word being processed and cursor positions

## Dependencies
- Functions called/Symbols referenced:
  - find_among_b: Searches backwards through the word for "e" or "l" suffixes
  - r_R2: Tests if the current position is within the R2 morphological region  
  - r_R1: Tests if the current position is within the R1 morphological region
  - r_shortv: Tests if the word ends with a short syllable pattern
  - slice_del: Deletes the identified character from the word
  - a_8: Array containing patterns for "e" and "l" characters
- Called from (representative examples):
  - english_ISO_8859_1_stem: Main English stemming function
  - english_UTF_8_stem: UTF-8 version of English stemming

## Notes and Other Information
- This is the final step (Step 5) in the Porter stemming algorithm
- The function implements complex logic to avoid removing "e" from words where it would change pronunciation (short syllable test)  
- For "l" removal, it specifically requires the preceding character to also be "l" (double-l pattern)
- Returns 1 on successful operation, 0 if no applicable pattern found
- Critical for maintaining word integrity while achieving proper morphological normalization