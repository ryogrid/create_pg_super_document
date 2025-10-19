# r_step4

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2975-3004](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L2975-L3004)

## Overview
A complex step function in the Greek Snowball stemmer that performs multi-phase suffix transformations with conditional vowel-based replacements and word-boundary validation.

## Definition
```c
static int r_step4(struct SN_env * z)
```

## Detailed Description
The `r_step4` function implements a sophisticated three-phase transformation process:

1. **Initial Suffix Removal**: Searches for specific patterns using the `a_33` array (4 patterns) and removes matching suffixes
2. **State Reset**: Sets `z->I[0] = 0` to clear any previous state information
3. **Conditional Vowel Replacement**: Uses backtracking logic (`m1` position marker) to attempt vowel-based replacement:
   - Checks if current character is a Greek vowel using `in_grouping_b_U` with range 945-969
   - If vowel found, replaces with "ικ" (ik, s_70)
   - If no vowel, restores position and continues
4. **Final Suffix Processing**: Searches for patterns from the large `a_34` array (36 patterns) but only at word beginnings (`z->c > z->lb` check fails)
5. **Final Replacement**: Replaces matched pattern with "ικ" (ik, s_71)

The function uses sophisticated backtracking and position management to handle complex Greek morphological transformations.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure (`struct SN_env`) containing:
  - `z->c`: Current position in the string being processed
  - `z->ket`: End position of the substring being matched
  - `z->bra`: Start position of the substring being matched  
  - `z->lb`: Left boundary of the string
  - `z->l`: Length of the string
  - `z->I[0]`: Integer state variable that gets reset to 0

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Searches backwards for patterns in the given array
  - [slice_del](../s/slice_del.md): Deletes the substring between bra and ket
  - [slice_from_s](../s/slice_from_s.md): Replaces the substring with specified string
  - [in_grouping_b_U](../i/in_grouping_b_U.md): Checks if character belongs to specified Unicode group backwards
  - `a_33`: Array of 4 suffix patterns for initial matching
  - `a_34`: Array of 36 suffix patterns for final processing
  - `g_v`: Greek vowel grouping definition
  - `s_70`: Greek string "ικ" (ik) for conditional replacement
  - `s_71`: Greek string "ικ" (ik) for final replacement
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md): Main Greek stemming function at line 3571

## Notes and Other Information
- This is step 4 in the Greek stemming algorithm, featuring the most complex logic of the step functions processed
- Uses backtracking with position markers (`m1`) to handle conditional transformations
- Both s_70 and s_71 contain the same "ικ" suffix, suggesting standardized morphological endings
- The word-boundary check ensures final transformations only apply at word roots
- Returns 1 on successful transformation, 0 if conditions not met, or negative values on error
- The goto-based control flow implements a state machine for handling multiple transformation paths

## Simplified Source

```c
static int r_step4(struct SN_env * z) {
    // Phase 1: Find and remove suffix from a_33 patterns (4 patterns)
    z->ket = z->c;
    if (!find_among_b(z, a_33, 4)) return 0;
    z->bra = z->c;
    slice_del(z);  // Remove the matched suffix

    // Reset state variable
    z->I[0] = 0;

    // Phase 2: Try vowel-based replacement with backtracking
    int saved_position = z->l - z->c;  // Save current position
    z->ket = z->c;
    z->bra = z->c;

    // If current character is a Greek vowel, replace with "ικ"
    if (!in_grouping_b_U(z, g_v, 945, 969, 0)) {
        slice_from_s(z, 4, s_70);  // Replace with "ικ" (ik)
    } else {
        // Restore position if no vowel found
        z->c = z->l - saved_position;
        z->ket = z->c;
    }

    // Phase 3: Final suffix processing at word boundary
    z->bra = z->c;
    if (!find_among_b(z, a_34, 36)) return 0;  // 36 patterns in a_34

    // Only proceed if at word beginning
    if (z->c > z->lb) return 0;

    // Final replacement with "ικ"
    slice_from_s(z, 4, s_71);

    return 1;  // Success
}
```