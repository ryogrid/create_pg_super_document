# r_Step_4

## Location
src/backend/snowball/libstemmer/stem_UTF_8_porter.c: 480 - 514

## Overview
The r_Step_4 function implements Step 4 of the Porter stemming algorithm, which removes common suffixes when the resulting stem would be within the R2 morphological region, ensuring conservative suffix removal.

## Definition
```c
static int r_Step_4(struct SN_env * z)
```

## Detailed Description
This function handles the fourth step of the Porter stemming algorithm, focusing on removing a variety of common suffixes but only when the remaining stem would be within the more restrictive R2 region. This conservative approach prevents over-stemming by ensuring that only words with sufficiently long stems undergo transformation.

The function uses lookup table a_7 containing 18 different suffix patterns. It implements two main cases: simple deletion (case 1) and conditional deletion for suffixes ending in 'ion' (case 2). For case 2, it checks if the character preceding 'ion' is either 's' or 't', which is a specific requirement for removing '-sion' and '-tion' endings.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the stemming environment, including:
  - `c`: Current cursor position in the string
  - `l`: Length of the string  
  - `lb`: Left boundary (start of processable region)
  - `p`: Pointer to the character array being processed
  - `ket`: End position marker for substring operations
  - `bra`: Beginning position marker for substring operations

## Dependencies
- Functions called/Symbols referenced:
  - find_among_b (searches for suffix patterns in lookup table a_7)
  - r_R2 (tests if current position is within R2 morphological region)
  - slice_del (deletes the matched substring)
- Called from (representative examples):
  - english_ISO_8859_1_stem
  - porter_ISO_8859_1_stem
  - english_UTF_8_stem
  - porter_UTF_8_stem

## Notes and Other Information
- Returns 1 on successful transformation, 0 if no applicable suffix was found or R2 constraint not met
- Uses lookup table a_7 containing 18 different suffix patterns
- Exclusively requires R2 region constraint, making this the most conservative stemming step
- Case 2 specifically handles '-ion' suffixes with special logic for preceding 's' or 't' characters
- The character filtering optimization quickly rejects words without potential target suffixes
- Handles suffixes like '-ment', '-ness', '-ance', '-ence', '-able', '-ible', '-ant', '-ent', etc.
- Critical for preventing over-stemming of shorter words while effectively reducing longer derivatives
- Works as the primary suffix removal step for most derivational endings in the Porter algorithm