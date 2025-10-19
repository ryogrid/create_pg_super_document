# r_R2

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_basque.c:991-995](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_basque.c#L991-L995)

## Overview
This function tests whether the current cursor position is within the R2 (Region 2) boundary, which is the most restrictive morphological region for Snowball stemming algorithms.

## Definition
```c
static int r_R2(struct SN_env * z)
```

## Detailed Description
The `r_R2` function is a critical boundary checker that determines if the current position in the word being processed falls within the R2 region. The R2 region represents the most restrictive morphological boundary in the Snowball algorithm, established by finding the second consonant-vowel sequence in the word structure.

This function performs a single comparison to check if the current cursor position (`z->c`) is at or beyond the R2 boundary marker (`z->I[0]`) that was previously set during the region marking phase. The R2 test is typically used for the most conservative stemming operations, ensuring that only words with sufficient morphological complexity undergo certain suffix removals.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing:
  - `z->c`: Current cursor position in the word
  - `z->I[0]`: The R2 region boundary position (set by r_mark_regions)

## Dependencies
- Functions called/Symbols referenced: None (simple boundary check)
- Called from (representative examples):
  - [r_aditzak](r_aditzak.md) (Basque verb processing)
  - [r_izenak](r_izenak.md) (Basque noun processing)
  - [r_standard_suffix](r_standard_suffix.md) (across multiple Romance languages)
  - [r_Step_3](r_Step_3.md), r_Step_4, r_Step_5 (English Porter stemmer steps)
  - [r_verb_suffix](r_verb_suffix.md) (French/Catalan verb processing)
  - Language-specific suffix removal functions across all supported languages

## Notes and Other Information
- Returns 1 if cursor is within R2 region, 0 otherwise
- Most restrictive of the three region tests (R2 < R1 < RV in terms of position)
- Essential for preventing over-stemming in morphologically complex words
- Used extensively across all language implementations in the Snowball library
- Critical for maintaining precision in languages with rich inflectional morphology
- The R2 region typically contains only the core morphological stem of words

## Simplified Source

```c
static int r_R2(struct SN_env * z) {
    // Check if current cursor position is within the R2 region (most restrictive)
    return (z->I[0] <= z->c) ? 1 : 0;
}
```