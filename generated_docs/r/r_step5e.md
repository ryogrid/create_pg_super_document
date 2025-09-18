# r_step5e

## Location
src/backend/snowball/libstemmer/stem_UTF_8_greek.c: 3176 - 3194

## Overview
A static function in the Greek stemmer that performs step 5e of the Greek stemming algorithm, handling specific long morphological patterns that require 4-to-10 character expansion in Greek words.

## Definition
```c
static int r_step5e(struct SN_env * z)
```

## Detailed Description
The r_step5e function is part of the Greek language stemming algorithm implementation in PostgreSQL's snowball stemmer library. This function performs step 5e of the Greek stemming process, which involves:

1. **Long Pattern Detection**: Checks for specific Greek patterns ending with Unicode character 181, with a minimum length requirement of 11 characters (c - 11 <= lb)
2. **Pattern Matching**: Uses the a_44 lookup table (2 entries) to identify specific morphological patterns
3. **Pattern Deletion**: Removes the matched pattern from the word
4. **Expansion Substitution**: Performs a 4-character to 10-character substitution (s_88 → s_89), which is unusual as most stemming operations reduce word length

This step is distinctive because it expands rather than reduces the word, suggesting it handles specific Greek morphological transformations where a shorter form needs to be converted to a longer canonical form.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing:
  - `c`: Current cursor position in the string
  - `l`: Length of the string being processed  
  - `lb`: Left boundary for processing
  - `p`: Pointer to the string buffer
  - `ket`: End position marker for substring operations
  - `bra`: Start position marker for substring operations
  - `I[0]`: Integer array for storing intermediate results

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward pattern matching function
  - [slice_del](../s/slice_del.md): Function to delete a substring slice
  - [slice_from_s](../s/slice_from_s.md): Function to replace slice with specific string
  - [eq_s_b](../e/eq_s_b.md): Backward string equality check function
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md): Main Greek stemming function

## Notes and Other Information
- This function is specific to Greek language morphology and handles UTF-8 encoded Greek text
- Uses a small lookup table (a_44 with only 2 entries), indicating very specific Greek morphological cases
- Requires longer input words (minimum 11 characters) compared to other steps
- Performs word expansion (4 to 10 characters), which is atypical for stemming algorithms
- Returns 1 on successful pattern matching and substitution, 0 if required patterns don't match, or negative values on error
- Part of the sequential Greek stemming pipeline, executed in the later stages of the stemming process
- The expansion behavior suggests this step handles Greek morphological normalization rather than traditional stemming reduction