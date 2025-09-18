# r_Step_1

## Location
src/backend/snowball/libstemmer/stem_UTF_8_serbian.c: 5130 - 5604

## Overview
Performs the first major step of Serbian morphological suffix removal, handling 130 different suffix patterns with their corresponding root replacements.

## Definition
```c
static int r_Step_1(struct SN_env * z)
```

## Detailed Description
This function represents the primary morphological transformation step in the Serbian stemming algorithm. It processes the most common and important Serbian suffixes by:

1. **Suffix Recognition**: Uses `find_among_b()` with array `a_1` to scan backwards from the end of the word, matching against 130 predefined Serbian suffix patterns.

2. **Morphological Transformations**: For each matched suffix (cases 1-91), applies the appropriate root transformation using `slice_from_s()`. These transformations include:
   - Adjective suffixes: Converting adjectival forms back to root forms
   - Noun suffixes: Handling case endings, diminutives, and plural forms
   - Verb suffixes: Processing verbal conjugations and participles
   - Comparative and superlative forms
   - Gender and number variations

3. **Accent-Sensitive Rules**: Some transformations (cases 7, 31, 52, 55, 57, 65, 72, 91) include checks for the accent boundary marker `z->I[1]`, ensuring that morphological operations respect Serbian stress patterns.

4. **Root Preservation**: The function replaces complex suffixed forms with simpler root forms (e.g., "loga" → root, "peh" → root, "vojno" → "vojka", "bojno" → "bojka").

This step is crucial for Serbian text processing as it handles the rich morphological system of Serbian, where words can have multiple suffixed forms that should all reduce to the same conceptual root.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing:
  - `c`: Current cursor position (moves backward during suffix matching)
  - `bra`: Start boundary of matched suffix
  - `ket`: End boundary of matched suffix  
  - `I[1]`: Accent boundary marker for accent-sensitive rules
  - `p`: Pointer to the string buffer
  - `lb`: Left boundary limit for matching

## Dependencies
- Functions called/Symbols referenced:
  - `find_among_b`: Backward suffix matching against suffix array `a_1`
  - `slice_from_s`: Replaces matched suffix with root form
- Called from (representative examples):
  - `serbian_UTF_8_stem`: Main Serbian stemming function

## Notes and Other Information
- Auto-generated function from Snowball stemming language specification
- Handles the most comprehensive set of Serbian morphological transformations
- The 130 suffix patterns cover major Serbian morphological categories including nouns, adjectives, verbs, and their inflected forms
- Some rules are accent-sensitive, using the `z->I[1]` boundary set by `r_mark_regions()`
- Critical first step that must succeed before subsequent morphological steps (`r_Step_2`, `r_Step_3`)
- Returns 1 if a suffix was successfully matched and transformed, 0 if no applicable suffix was found
- Essential component of Serbian language stemming, handling the complex inflectional morphology of Serbian