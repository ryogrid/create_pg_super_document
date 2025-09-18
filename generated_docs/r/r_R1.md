# r_R1

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_1_basque.c: 996 - 1000

## Overview
This function tests whether the current cursor position is within the R1 (Region 1) boundary, which is an intermediate morphological region for Snowball stemming algorithms.

## Definition
```c
static int r_R1(struct SN_env * z)
```

## Detailed Description
The `r_R1` function is a morphological boundary checker that determines if the current position in the word being processed falls within the R1 region. The R1 region represents an intermediate level of morphological restriction between the RV (most permissive) and R2 (most restrictive) regions in the Snowball algorithm.

This function performs a single comparison to check if the current cursor position (`z->c`) is at or beyond the R1 boundary marker (`z->I[1]`) that was previously established during the region marking phase. The R1 test is used for moderate stemming operations, allowing suffix removal in words that have adequate morphological complexity but not requiring the most conservative R2 boundary.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing:
  - `z->c`: Current cursor position in the word
  - `z->I[1]`: The R1 region boundary position (set by r_mark_regions)

## Dependencies
- Functions called/Symbols referenced: None (simple boundary check)
- Called from (representative examples):
  - r_izenak (Basque noun processing)
  - r_Step_1b, r_Step_2, r_Step_3, r_Step_5 (English Porter stemmer steps)
  - r_attached_pronoun (Catalan/Spanish pronoun processing)
  - r_standard_suffix (across multiple Romance languages)
  - Language-specific morphological processing functions (Hungarian case endings, Irish verb suffixes)
  - Various suffix removal functions across all supported languages

## Notes and Other Information
- Returns 1 if cursor is within R1 region, 0 otherwise
- Intermediate restriction level: RV ⊃ R1 ⊃ R2 (in terms of position within words)
- Balances stemming effectiveness with precision by allowing moderate suffix removal
- Used extensively across all major language families in the Snowball library
- Essential for handling languages with moderate inflectional complexity
- Particularly important for Germanic, Romance, and Slavic language processing
- Enables context-sensitive suffix removal based on word structure analysis