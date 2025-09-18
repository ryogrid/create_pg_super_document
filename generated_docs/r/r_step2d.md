# r_step2d

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2939-2957](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L2939-L2957)

## Overview
A step function in the Greek Snowball stemmer that performs suffix transformations during the second phase of stemming, with special handling for word-beginning patterns and state variable manipulation.

## Definition
```c
static int r_step2d(struct SN_env * z)
```

## Detailed Description
The `r_step2d` function is part of the Greek language stemmer that performs a complex two-phase transformation with state management:

1. **Phase 1**: Searches for specific suffix patterns using the `a_30` array (2 patterns) and removes matching suffixes
2. **State Reset**: Sets the integer variable `z->I[0]` to 0, likely for tracking stemming state
3. **Phase 2**: Searches for patterns from the `a_31` array (8 patterns) but only if the current position reaches the left boundary (`z->c > z->lb` check fails)
4. **Replacement**: Replaces the matched pattern with the Greek character "ε" (epsilon, s_68)

The function requires at least 5 characters before the left boundary and validates that the character at position `c-1` is either 131 (0x83) or 189 (0xBD) in UTF-8 encoding, similar to r_step2c.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure (`struct SN_env`) containing:
  - `z->c`: Current position in the string being processed
  - `z->ket`: End position of the substring being matched
  - `z->bra`: Start position of the substring being matched  
  - `z->lb`: Left boundary of the string
  - `z->p`: Pointer to the string data
  - `z->I[0]`: Integer state variable that gets reset to 0

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Searches backwards for patterns in the given array
  - [slice_del](../s/slice_del.md): Deletes the substring between bra and ket
  - [slice_from_s](../s/slice_from_s.md): Replaces the substring with specified string
  - `a_30`: Array of 2 suffix patterns for initial matching
  - `a_31`: Array of 8 suffix patterns for replacement phase
  - `s_68`: Greek character "ε" (epsilon) used as replacement
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md): Main Greek stemming function at line 3559

## Notes and Other Information
- This is step 2d in the Greek stemming algorithm, executed after step 2c
- The function includes state management through `z->I[0] = 0`, indicating it may affect subsequent stemming steps
- The boundary check (`z->c > z->lb`) ensures that matches only occur at word beginnings
- Returns 1 on successful transformation, 0 if no match found, or negative values on error
- The shorter minimum length requirement (5 vs 9 characters) suggests it handles shorter suffix patterns than r_step2c