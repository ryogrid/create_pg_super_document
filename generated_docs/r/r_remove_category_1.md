# r_remove_category_1

## Location
src/backend/snowball/libstemmer/stem_UTF_8_nepali.c: 284 - 318

## Overview
Removes category 1 suffixes from Nepali words as part of the Snowball stemming algorithm for UTF-8 encoded text.

## Definition
```c
static int r_remove_category_1(struct SN_env * z)
```

## Detailed Description
This function is part of the Nepali language stemming implementation in PostgreSQL's Snowball stemmer. It processes the word backwards from the current cursor position, attempting to match against 17 predefined suffix patterns in the a_0 array. The function uses a two-case switch statement to handle different removal strategies:

- Case 1: Simple deletion of the matched suffix
- Case 2: Conditional deletion based on checking for specific preceding patterns (s_0 and s_1 strings)

The function operates on the word boundary markers (ket and bra) and uses backtracking logic with labeled jumps to handle complex pattern matching scenarios.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the stemming environment, including the string being processed, cursor positions, and boundary markers

## Dependencies
- Functions called/Symbols referenced:
  - find_among_b (for pattern matching in suffix array a_0)
  - slice_del (for suffix removal)
  - eq_s_b (for backward string equality checking)
- Called from:
  - nepali_UTF_8_stem (main stemming function)

## Notes and Other Information
- This is a static function, only accessible within the Nepali stemmer module
- Uses goto statements and labels for efficient backtracking in pattern matching
- Part of the automatically generated code from Snowball stemming rules
- Returns 1 on successful pattern match and processing, 0 if no pattern matched
- Error conditions from slice_del are propagated upward