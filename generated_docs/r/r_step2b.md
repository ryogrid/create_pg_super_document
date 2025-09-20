# r_step2b

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2904-2921](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L2904-L2921)

## Overview
A static function that implements step 2b of the Greek language stemming algorithm, performing two-stage suffix removal and replacement with character validation at both stages.

## Definition
static int r_step2b(struct SN_env * z)

## Detailed Description
The r_step2b function performs morphological transformations for step 2b in Greek stemming through a dual validation and processing approach:

1. Sets the cursor position (ket) to current position
2. Performs bounds checking (minimum 7 characters from left boundary)
3. Validates that the character at position c-1 is either 131 or 189 (specific Greek UTF-8 character codes)
4. Uses find_among_b to search backward through predefined suffix patterns (a_26 array with 2 entries)
5. If a match is found, deletes the matched suffix using slice_del
6. Performs a second validation stage with different bounds checking (minimum 3 characters)
7. Validates that the character at position c-1 is either 128 or 187 (different Greek UTF-8 character codes)
8. Uses find_among_b with a_27 array (8 entries) for second pattern matching
9. Replaces the matched pattern with a 4-character string (s_66)

The function implements a mandatory two-stage process with different character validation requirements at each stage.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment, including:

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward string pattern matching)
  - [slice_del](../s/slice_del.md) (suffix deletion)
  - [slice_from_s](../s/slice_from_s.md) (string replacement)
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md)

## Notes and Other Information
- Returns 1 on successful execution, 0 on validation failure or no pattern match
- Part of automatically generated Snowball stemmer code for Greek language
- Uses two different sets of character validation codes: (131, 189) and (128, 187)
- Implements mandatory two-stage processing with different boundary requirements (7 vs 3 characters)
- Uses predefined arrays (a_26, a_27) and string constant (s_66)
- Companion function to r_step2a, handling different morphological patterns in step 2