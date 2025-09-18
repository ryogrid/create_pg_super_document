# r_remove_category_2

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_nepali.c:327-365](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_nepali.c#L327-L365)

## Overview
Removes category 2 suffixes from Nepali words with complex conditional pattern matching as part of the Snowball stemming algorithm.

## Definition
```c
static int r_remove_category_2(struct SN_env * z)
```

## Detailed Description
This function handles the removal of category 2 suffixes in the Nepali stemming process. It begins with the same UTF-8 character validation as r_check_category_2, then performs pattern matching against the a_2 suffix array containing 3 patterns.

The function implements a two-case switch statement with sophisticated conditional logic:

- Case 1: Sequential pattern matching with backtracking - tests against four different 6-character patterns (s_2, s_3, s_4, s_5) using goto-based control flow. If any pattern matches, the suffix is deleted.
- Case 2: Simple pattern matching against a 9-character pattern (s_6) followed by deletion if matched.

The backtracking mechanism allows the function to test multiple alternative patterns before deciding whether to proceed with suffix removal.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the stemming environment, including the string being processed, cursor positions, and boundary markers

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (for pattern matching in suffix array a_2)
  - [slice_del](../s/slice_del.md) (for suffix removal)
  - [eq_s_b](../e/eq_s_b.md) (for backward string equality checking against patterns s_2 through s_6)
- Called from:
  - [nepali_UTF_8_stem](../n/nepali_UTF_8_stem.md) (main stemming function)

## Notes and Other Information
- This is a static function, only accessible within the Nepali stemmer module
- Uses the same UTF-8 character validation as r_check_category_2 with bitmask operation (262 >> (z->p[z->c - 1] & 0x1f)) & 1)
- Implements complex backtracking logic with labeled jumps for efficient pattern testing
- Part of the automatically generated code from Snowball stemming rules
- Returns 1 on successful pattern match and processing, 0 if no valid pattern found
- Error conditions from slice_del are propagated upward