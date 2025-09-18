# r_remove_category_3

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_nepali.c:366-375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_nepali.c#L366-L375)

## Overview
Removes category 3 suffixes from Nepali words using simple pattern matching and deletion in the Snowball stemming algorithm.

## Definition
```c
static int r_remove_category_3(struct SN_env * z)
```

## Detailed Description
This function handles the removal of category 3 suffixes in the Nepali stemming process. It implements the simplest suffix removal logic among the category functions, performing straightforward pattern matching against a large array of 91 predefined suffix patterns (a_3 array).

The function operates by:
1. Setting the ket boundary marker to the current cursor position
2. Searching backwards through the a_3 suffix array containing 91 different patterns
3. If a match is found, setting the bra boundary marker and deleting the matched suffix
4. Returning success (1) if a pattern was matched and removed, failure (0) if no pattern matched

Unlike categories 1 and 2, this function does not implement conditional logic or complex pattern validation - it performs direct matching and removal.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the stemming environment, including the string being processed, cursor positions, and boundary markers

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (for pattern matching in suffix array a_3)
  - [slice_del](../s/slice_del.md) (for suffix removal)
- Called from:
  - [nepali_UTF_8_stem](../n/nepali_UTF_8_stem.md) (main stemming function)

## Notes and Other Information
- This is a static function, only accessible within the Nepali stemmer module
- Handles the largest set of suffix patterns (91) among all category removal functions
- Uses simple match-and-delete logic without additional conditional checks
- Part of the automatically generated code from Snowball stemming rules
- Returns 1 on successful pattern match and removal, 0 if no pattern matched
- Error conditions from slice_del are propagated upward