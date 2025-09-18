# r_steps7

## Location
src/backend/snowball/libstemmer/stem_UTF_8_greek.c: 2769 - 2788

## Overview
The r_steps7 function is part of the Greek stemming algorithm in PostgreSQL's snowball stemmer library, implementing the final step 7 of the stemming process with specific character-based pattern matching and transformation.

## Definition
```c
static int r_steps7(struct SN_env * z)
```

## Detailed Description
This function implements step 7 of the Greek stemming algorithm through a two-phase character-specific pattern matching process:

1. **First Phase**: Character and pattern validation:
   - Sets ket position to current cursor
   - Performs bounds checking (z->c - 9 <= z->lb) to ensure sufficient characters
   - Checks for specific characters 177 (±) or 185 (¹) at position c-1
   - Searches for patterns from array a_16 (4 patterns)
   - If successful, deletes the matched slice and resets counter I[0] to 0

2. **Second Phase**: Final transformation:
   - Sets both ket and bra positions to current cursor
   - Performs additional bounds checking (z->c - 1 <= z->lb)
   - Checks for specific characters 131 (ƒ) or 135 (‡) at position c-1
   - Searches for patterns from array a_15 (2 patterns)
   - Replaces the matched portion with an 8-character predefined string (s_57)

The function serves as the final cleanup step in the Greek stemming process with very specific Unicode character requirements.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing:
  - `ket`: End position marker for pattern matching
  - `bra`: Start position marker for pattern matching
  - `c`: Current cursor position
  - `lb`: Lower bound for matching
  - `p`: Pointer to the string buffer
  - `I[0]`: Integer counter array (element 0 reset to 0)

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward pattern matching)
  - [slice_del](../s/slice_del.md) (slice deletion)
  - [slice_from_s](../s/slice_from_s.md) (slice replacement with predefined string)
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md) (main stemming function)

## Notes and Other Information
- This is a static function, only accessible within the Greek stemmer module
- Uses predefined pattern arrays (a_15, a_16) and replacement string (s_57)
- Returns 1 on success, 0 on no match, or negative values on error
- The final step in the sequential Greek stemming algorithm
- Uses very specific Unicode character filtering (177, 185, 131, 135) for Greek text processing
- Includes comprehensive bounds checking to prevent buffer underruns
- More restrictive than earlier steps, requiring exact character matches at specific positions
- Performs the final morphological cleanup in the Greek stemming process