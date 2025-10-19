# r_step5h

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3274-3303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L3274-L3303)

## Overview
A static function within the Greek language stemmer that implements step 5h of the Greek stemming algorithm, performing specific suffix removal and replacement operations with two alternative replacement strategies.

## Definition
```c
static int r_step5h(struct SN_env * z)
```

## Detailed Description
The `r_step5h` function is part of the Snowball stemming algorithm implementation for Greek text processing. It follows a structured approach:

1. **Suffix Detection and Removal**: Searches for suffixes from array `a_53` (3 entries) and removes them if found, then sets the stemmer state flag `z->I[0]` to 0.

2. **Two-Phase Replacement Strategy**: After suffix removal, attempts two mutually exclusive replacement patterns:
   - **First Option**: Tries to match patterns from array `a_51` (12 entries) and replaces with string `s_96` (6 characters)
   - **Second Option**: If the first option fails, tries to match patterns from array `a_52` (25 entries), ensures the cursor is not beyond the left boundary, and replaces with string `s_97` (6 characters)

The function uses cursor management and boundary checking to ensure safe string operations within the Snowball environment.

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
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md) at src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3619

## Notes and Other Information
- This function is part of the automatically generated Snowball stemmer code for Greek language
- Returns 1 on successful completion, 0 if required patterns are not found, or negative values on errors
- The function modifies the input string in-place by removing/replacing suffixes
- Uses multiple suffix lookup arrays (a_51, a_52, a_53) with different sizes (3, 12, and 25 entries respectively)
- Both replacement strings (s_96, s_97) are 6 characters long
- Includes boundary checking (`z->c > z->lb`) to prevent invalid cursor positions
- The two replacement strategies are mutually exclusive - only one will be executed per function call

## Simplified Source

```c
static int r_step5h(struct SN_env * z) {
    // Step 1: Find and remove suffix from pattern set a_53
    z->ket = z->c;
    if (!find_among_b(z, a_53, 3)) return 0;
    z->bra = z->c;
    slice_del(z);  // Remove found suffix

    // Reset stemmer state
    z->I[0] = 0;

    // Step 2: Try first replacement strategy
    int saved_pos = z->l - z->c;
    z->ket = z->c;
    z->bra = z->c;
    if (find_among_b(z, a_51, 12)) {
        slice_from_s(z, 6, s_96);  // Replace with 6-char string s_96
        return 1;
    }

    // Step 3: Try second replacement strategy if first failed
    z->c = z->l - saved_pos;
    z->ket = z->c;
    z->bra = z->c;
    if (!find_among_b(z, a_52, 25)) return 0;
    if (z->c > z->lb) return 0;  // Boundary check
    slice_from_s(z, 6, s_97);    // Replace with 6-char string s_97

    return 1;
}
```