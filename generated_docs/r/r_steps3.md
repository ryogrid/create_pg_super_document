# r_steps3

## Location
src/backend/snowball/libstemmer/stem_UTF_8_greek.c: 2588 - 2627

## Overview
The r_steps3 function is part of the Greek stemming algorithm in PostgreSQL's snowball stemmer library, responsible for performing specific suffix removal and transformation steps during the stemming process.

## Definition
```c
static int r_steps3(struct SN_env * z)
```

## Detailed Description
This function implements step 3 of the Greek stemming algorithm. It performs pattern matching and transformation on Greek text by:

1. Setting the ket position to the current cursor position
2. Using find_among_b to search for patterns from array a_7 (7 patterns)
3. If a match is found, deleting the matched slice
4. Resetting counter I[0] to 0
5. Attempting to match specific patterns (s_38) and performing conditional replacements
6. If the first pattern match fails, it falls back to searching patterns from array a_6 (32 patterns)
7. Based on the matched pattern (among_var), it performs different slice replacements using predefined strings (s_40, s_41)

The function uses backward matching (indicated by the '_b' suffix in function calls) to process suffixes from the end of words.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing:
  - `ket`: End position marker for pattern matching
  - `bra`: Start position marker for pattern matching
  - `c`: Current cursor position
  - `l`: Length/limit of the string
  - `lb`: Lower bound for matching
  - `I[0]`: Integer counter array (element 0 reset to 0)

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward pattern matching)
  - [slice_del](../s/slice_del.md) (slice deletion)
  - [eq_s_b](../e/eq_s_b.md) (backward string equality check)
  - [slice_from_s](../s/slice_from_s.md) (slice replacement)
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md) (main stemming function)

## Notes and Other Information
- This is a static function, only accessible within the Greek stemmer module
- Uses predefined pattern arrays (a_6, a_7) and replacement strings (s_38, s_39, s_40, s_41)
- Returns 1 on success, 0 on no match, or negative values on error
- Part of a multi-step stemming algorithm where each step handles different morphological patterns
- Uses goto statements for control flow, which is common in generated snowball code