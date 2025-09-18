# danish_ISO_8859_1_stem

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_1_danish.c: 274 - 310

## Overview
The main stemming function for Danish text using ISO 8859-1 encoding that applies the complete Danish stemming algorithm to reduce words to their root forms.

## Definition
```c
extern int danish_ISO_8859_1_stem(struct SN_env * z)
```

## Detailed Description
This function implements the complete Danish stemming algorithm for words encoded in ISO 8859-1. It follows a sequential process that first identifies word regions, then applies various suffix removal rules, handles consonant pairs, processes additional suffixes, and finally removes doubled consonants. The function operates on a Snowball environment structure that maintains the current word state and cursor positions throughout the stemming process.

The stemming process follows these steps:
1. Mark vowel/consonant regions in the word
2. Remove main suffixes 
3. Handle consonant pair reductions
4. Process other suffixes
5. Remove doubled consonants (undoubling)

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the word to be stemmed and cursor state information

## Dependencies
- Functions called/Symbols referenced:
  - r_mark_regions
  - r_main_suffix
  - r_consonant_pair
  - r_other_suffix
  - r_undouble
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Returns 1 on successful completion, negative values on error
- Preserves original cursor position using temporary variables (c1, m2-m5)
- Part of the Snowball stemming library implementation for Danish language
- Uses backward processing (from end of word) for most suffix operations
- Located in stem_ISO_8859_1_danish.c:274-310