# r_step5j

## Location
src/backend/snowball/libstemmer/stem_UTF_8_greek.c: 3353 - 3371

## Overview
A static function within the Greek language stemmer that implements step 5j of the Greek stemming algorithm, performing suffix removal followed by character-specific pattern matching and replacement.

## Definition
```c
static int r_step5j(struct SN_env * z)
```

## Detailed Description
The `r_step5j` function is part of the Snowball stemming algorithm implementation for Greek text processing. It follows a two-phase approach with strict character validation:

1. **Suffix Detection and Removal**: Searches for suffixes from array `a_57` (3 entries) and removes them if found, then sets the stemmer state flag `z->I[0]` to 0.

2. **Character-Specific Pattern Matching**: After suffix removal, performs a precise replacement operation with multiple validation checks:
   - Validates that the character at position `c-1` is specifically character code 189 (a specific Greek character in UTF-8)
   - Searches for patterns in array `a_58` (6 entries)
   - Ensures the cursor position does not exceed the left boundary (`z->c > z->lb`)
   - Replaces the matched pattern with 4-character string `s_102`

This function is more restrictive than other step5 functions, requiring both pattern matching and specific character validation.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure (`struct SN_env`) containing:
  - `c`: Current cursor position
  - `l`: Length/end position of the string
  - `lb`: Left boundary position
  - `bra`, `ket`: Substring boundaries for operations
  - `I[0]`: Integer state variable used by the stemming algorithm
  - `p`: Pointer to the string being processed

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward search for suffix patterns in arrays
  - [slice_del](../s/slice_del.md): Delete the substring between bra and ket
  - [slice_from_s](../s/slice_from_s.md): Replace substring with a specific string
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md) at src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3625

## Notes and Other Information
- This function is part of the automatically generated Snowball stemmer code for Greek language
- Returns 1 on successful completion, 0 if required patterns/conditions are not met, or negative values on errors
- The function modifies the input string in-place by removing/replacing suffixes
- Uses two suffix lookup arrays (a_57, a_58) with 3 and 6 entries respectively
- Character code 189 represents a specific Greek character marker in the UTF-8 encoding scheme
- More restrictive than other step5 functions due to the specific character validation requirement
- Multiple boundary and character checks ensure safe operations and prevent invalid cursor positions
- The replacement string s_102 is 4 characters long
- This step appears to handle a specific Greek linguistic pattern that requires precise character-level validation