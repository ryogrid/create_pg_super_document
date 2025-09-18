# r_RV

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_basque.c:986-990](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_basque.c#L986-L990)

## Overview
This function is a predicate that tests whether the current cursor position is within the RV (R Voyelle/vowel) region for Snowball stemming algorithms.

## Definition
```c
static int r_RV(struct SN_env * z)
```

## Detailed Description
The `r_RV` function is a simple boundary checker that determines if the current position in the word being processed falls within the RV (R Voyelle) region. The RV region is a morphological boundary established by the `r_mark_regions` function and represents the vowel-based region where certain stemming operations can be safely performed.

The function performs a single comparison to check if the current cursor position (`z->c`) is at or beyond the RV boundary marker (`z->I[2]`) that was previously set during region marking. This is a critical test used throughout the stemming process to ensure suffix removal operations only occur in appropriate morphological contexts.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing:
  - `z->c`: Current cursor position in the word
  - `z->I[2]`: The RV region boundary position (set by r_mark_regions)

## Dependencies
- Functions called/Symbols referenced: None (simple boundary check)
- Called from (representative examples):
  - [r_aditzak](r_aditzak.md) (Basque verb processing)
  - [r_izenak](r_izenak.md) (Basque noun processing)  
  - [r_adjetiboak](r_adjetiboak.md) (Basque adjective processing)
  - [r_standard_suffix](r_standard_suffix.md) (French/Italian/Spanish suffix processing)
  - [r_attached_pronoun](r_attached_pronoun.md) (Italian/Spanish pronoun processing)
  - Multiple other suffix removal functions across Romance languages

## Notes and Other Information
- Returns 1 if cursor is within RV region, 0 otherwise
- Essential guard function preventing improper suffix removal
- Used extensively across Romance language stemmers (French, Italian, Spanish, Portuguese, Romanian)
- The RV region represents the core morphological area of words in these languages
- Simple but critical for maintaining stemming accuracy and preventing over-stemming