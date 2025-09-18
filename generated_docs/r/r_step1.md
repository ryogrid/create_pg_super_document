# r_step1

## Location
src/backend/snowball/libstemmer/stem_UTF_8_greek.c: 2474 - 2540

## Overview
The primary suffix removal function in the Greek stemming algorithm that handles the first phase of stemming by identifying and transforming common Greek suffixes using pattern matching.

## Definition
```c
static int r_step1(struct SN_env * z)
```

## Detailed Description
This function implements the core stemming logic for Greek text by performing suffix identification and replacement. It uses backward pattern matching (`find_among_b()`) with a predefined array (`a_1` containing 40 suffix patterns) to locate Greek suffixes at the end of words. Upon finding a match, it applies one of 11 different transformation rules through a switch statement.

Each transformation case replaces the matched suffix with a specific stem ending using predefined replacement strings (`s_24` through `s_34`) of varying lengths (4, 6, 8, 10, or 12 bytes). This systematic approach handles the complex morphology of Greek, where suffixes can indicate grammatical features like case, number, gender, and tense.

After successful suffix processing, the function sets `z->I[0] = 0`, which appears to be a flag or counter reset used by subsequent stemming steps to track processing state.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure (`struct SN_env *`) containing the word being stemmed, cursor positions, and state variables

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward pattern matching function for suffix identification
  - [slice_from_s](../s/slice_from_s.md): String replacement function using predefined constants
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md): Main Greek stemming function as part of the multi-step stemming process
  - [lithuanian_UTF_8_stem](../l/lithuanian_UTF_8_stem.md): Main Lithuanian stemming function (indicating shared stemming patterns)

## Notes and Other Information
- This represents the first and most comprehensive phase of Greek stemming, handling the majority of common suffixes
- The 40 different suffix patterns in array `a_1` cover a wide range of Greek morphological endings
- The 11 transformation cases provide different stem endings appropriate for different suffix types
- State management through `z->I[0] = 0` suggests coordination with subsequent stemming phases
- Returns 1 on successful suffix match and transformation, 0 if no applicable suffix is found
- Part of the multi-step Greek stemming algorithm that includes case conversion, multiple suffix removal phases, and cleanup
- Located in src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2474-2540