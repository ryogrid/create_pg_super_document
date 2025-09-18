# r_step5g

## Location
src/backend/snowball/libstemmer/stem_UTF_8_greek.c: 3232 - 3273

## Overview
A static function within the Greek language stemmer that implements step 5g of the Greek stemming algorithm, performing specific suffix removal and replacement operations.

## Definition
```c
static int r_step5g(struct SN_env * z)
```

## Detailed Description
The `r_step5g` function is part of the Snowball stemming algorithm implementation for Greek text processing. It performs two main phases of suffix processing:

1. **First Phase (Optional)**: Attempts to find and remove suffixes from array `a_47` (3 entries), then removes them and sets the stemmer state flag `z->I[0]` to 0.

2. **Second Phase (Required)**: Searches for suffixes from array `a_50` (3 entries) and removes them, also setting `z->I[0]` to 0.

3. **Replacement Logic**: After suffix removal, the function attempts two alternative replacement strategies:
   - First tries to match patterns from array `a_48` (6 entries) and replaces with string `s_94` (4 characters)
   - If that fails, tries to match patterns from array `a_49` (5 entries), checking for a specific character (184) at position `c-1`, and replaces with string `s_95` (4 characters)

The function uses the Snowball environment structure to manage cursor positions (`z->c`, `z->bra`, `z->ket`) and string boundaries (`z->l`, `z->lb`).

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
  - `find_among_b`: Backward search for suffix patterns in arrays
  - `slice_del`: Delete the substring between bra and ket
  - `slice_from_s`: Replace substring with a specific string
- Called from (representative examples):
  - `greek_UTF_8_stem` at src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3613

## Notes and Other Information
- This function is part of the automatically generated Snowball stemmer code for Greek language
- Returns 1 on successful completion, 0 if required patterns are not found, or negative values on errors
- The function modifies the input string in-place by removing/replacing suffixes
- Uses multiple suffix lookup arrays (a_47, a_48, a_49, a_50) and replacement strings (s_94, s_95)
- The character code 184 appears to be a specific Greek character marker in the UTF-8 encoding scheme