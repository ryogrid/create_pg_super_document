# r_Step_1c

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c: 612 - 635

## Overview
The r_Step_1c function implements Step 1c of the English Porter stemming algorithm, handling the transformation of 'y' or 'Y' suffixes to 'i' when preceded by a non-vowel.

## Definition
```c
static int r_Step_1c(struct SN_env * z)
```

## Detailed Description
This function performs a specific transformation rule in the English stemming algorithm:

1. **Suffix Detection**: Looks for words ending in 'y' or 'Y'
2. **Context Validation**: Ensures the 'y'/'Y' is preceded by a consonant (non-vowel)
3. **Position Check**: Verifies the consonant is not at the very beginning of the word
4. **Transformation**: Replaces the 'y'/'Y' with 'i'

This step is crucial for handling English words where 'y' functions as a vowel in certain contexts but needs to be normalized to 'i' for proper stemming (e.g., "happy" → "happi", "city" → "citi").

The function uses a branching approach to check both lowercase 'y' and uppercase 'Y', ensuring comprehensive coverage of input variations.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemming environment with:
  - : Current cursor position
  - : Length of the string
  - : Left boundary limit  
  - : Character array being processed
  - : End marker for current suffix
  - : Start marker for current suffix

## Dependencies
- Functions called/Symbols referenced:
  - out_grouping_b (checks if character is outside vowel group)
  - slice_from_s (replaces marked substring with specified string)
  - g_v (vowel character group: a,e,i,o,u,y range 97-121)
  - s_8 (replacement string: "i")
- Called from (representative examples):
  - english_ISO_8859_1_stem
  - porter_ISO_8859_1_stem
  - english_UTF_8_stem
  - porter_UTF_8_stem

## Notes and Other Information
- Returns 1 on successful transformation, 0 if no changes were made, or negative values on error
- Only transforms y/Y when preceded by a consonant and not at the word beginning
- This step normalizes the ambiguous nature of 'y' in English, where it can function as both consonant and vowel
- Essential for consistent stemming results in words ending with 'y' (cities → citi, flies → fli)
- Simple but important step in the Porter algorithm that improves stemming accuracy for y-final words