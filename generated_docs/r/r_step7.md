# r_step7

## Location
src/backend/snowball/libstemmer/stem_UTF_8_greek.c: 3450 - 3460

## Overview
A static function in the Greek stemmer that performs step 7 of the Greek language stemming algorithm, specifically targeting comparative and superlative forms ending in -τερ and -τατ patterns.

## Definition
```c
static int r_step7(struct SN_env * z)
```

## Detailed Description
This function performs step 7 of the Snowball Greek stemming algorithm, which specifically handles the removal of comparative and superlative suffixes in Greek. The function:

1. Sets the cursor position for suffix matching
2. Checks for minimum word length (7 characters) and specific UTF-8 byte endings (129 or 132)
3. Searches for patterns from the a_67 array containing comparative (-τερ) and superlative (-τατ) Greek suffixes
4. Deletes the matched suffix if found

The byte values 129 and 132 correspond to specific Greek UTF-8 character sequences that indicate potential comparative/superlative endings.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing:
  - `ket`: End position marker for substring operations
  - `bra`: Start position marker for substring operations  
  - `c`: Current cursor position
  - `lb`: Left boundary limit
  - `p`: Pointer to the string being processed

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward suffix matching)
  - [slice_del](../s/slice_del.md) (delete substring)
- Arrays used:
  - a_67 (8 Greek comparative/superlative patterns: εστερ, υτερ, ωτερ, οτερ, εστατ, υτατ, ωτατ, οτατ)
- Called from:
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md) at src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3661

## Notes and Other Information
- Returns 1 on successful processing, 0 if no transformation was applied
- Specifically targets Greek comparative and superlative forms
- Requires minimum word length of 7 characters to prevent over-stemming
- The byte value checks (129 and 132) are UTF-8 specific validations for Greek character sequences
- This step handles morphological forms like "καλύτερος" (better) and "καλύτατος" (best)
- Part of the final stages of the Greek stemming algorithm, handling complex morphological variations