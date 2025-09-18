# r_Step_2

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c: 636 - 728

## Overview
The r_Step_2 function implements Step 2 of the English Porter stemming algorithm, handling the removal and transformation of various adjectival and noun suffixes within the R1 region.

## Definition
```c
static int r_Step_2(struct SN_env * z)
```

## Detailed Description
This function performs Step 2 of the English stemming algorithm, which processes a comprehensive set of suffixes that form adjectives and nouns. The function operates exclusively within the R1 region to ensure stemming only occurs on substantial word stems.

The function handles 24 different suffix patterns through 15 transformation cases:

**Major suffix transformations**:
- **Case 1**:  →  
- **Cases 2-3**:  → 
- **Case 4**:  → 
- **Cases 5-7**:  → 
- **Cases 8-12**:  → 
- **Case 13**:  (preceded by 'l') → 
- **Case 14**:  → 
- **Case 15**:  (preceded by valid LI characters) → delete

The function uses bit manipulation (815616 mask) for efficient character classification and validates LI-ending suffixes against a specific character set.

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
  - find_among_b (searches backwards for matching suffixes)
  - r_R1 (tests if position is in R1 region)
  - slice_from_s (replaces marked substring with specified string)
  - slice_del (deletes the marked substring)
  - in_grouping_b (checks if character is in specified group)
  - a_5 (array of 24 suffixes: anci, enci, ogi, li, bli, abli, alli, fulli, lessli, ousli, entli, aliti, biliti, iviti, tional, ational, alism, ation, ization, izer, ator, iveness, fulness, ousness)
  - s_9 through s_22 (replacement strings: "tion", "ence", "ance", "able", "ent", "ize", "ate", "al", "ful", "ous", "ive", "ble", "og", "less")
  - g_valid_LI (character group for LI suffix validation)
- Called from (representative examples):
  - english_ISO_8859_1_stem
  - porter_ISO_8859_1_stem  
  - english_UTF_8_stem
  - porter_UTF_8_stem
  - serbian_UTF_8_stem

## Notes and Other Information
- Returns 1 on successful completion, 0 if no changes were made, or negative values on error
- All transformations require the suffix to be in the R1 region, preventing over-aggressive stemming
- Case 13 has additional constraint requiring 'l' before 'ogi' suffix
- Case 15 validates LI-ending against specific character set (c,d,e,g,h,k,m,n,r,t) to avoid incorrect deletions
- Critical component for handling complex English derivational morphology in PostgreSQL's full-text search capabilities