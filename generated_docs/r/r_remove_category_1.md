# r_remove_category_1

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_nepali.c:284-318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_nepali.c#L284-L318)

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
  - [find_among_b](../f/find_among_b.md) (for pattern matching in suffix array a_0)
  - [slice_del](../s/slice_del.md) (for suffix removal)
  - [eq_s_b](../e/eq_s_b.md) (for backward string equality checking)
- Called from:
  - [nepali_UTF_8_stem](../n/nepali_UTF_8_stem.md) (main stemming function)

## Notes and Other Information
- This is a static function, only accessible within the Nepali stemmer module
- Uses goto statements and labels for efficient backtracking in pattern matching
- Part of the automatically generated code from Snowball stemming rules
- Returns 1 on successful pattern match and processing, 0 if no pattern matched
- Error conditions from slice_del are propagated upward

## Simplified Source

```c
static int r_remove_category_1(struct SN_env * z) {
    // Set boundary for pattern matching
    z->ket = z->c;

    // Find which category 1 pattern matches (17 patterns in a_0 array)
    int pattern_id = find_among_b(z, a_0, 17);
    if (!pattern_id) return 0;

    z->bra = z->c;

    switch (pattern_id) {
        case 1:
            // Simple deletion of matched suffix
            slice_del(z);
            break;

        case 2:
            // Conditional deletion - check for specific preceding patterns
            int saved_pos = z->l - z->c;

            // Check if preceded by specific pattern s_0 or s_1
            if (eq_s_b(z, 3, s_0) || eq_s_b(z, 3, s_1)) {
                // Don't delete if preceded by these patterns
                break;
            }

            // Restore position and delete suffix
            z->c = z->l - saved_pos;
            slice_del(z);
            break;
    }

    return 1;
}
```