# r_steps6

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2676-2768](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L2676-L2768)

## Overview
The r_steps6 function is part of the Greek stemming algorithm in PostgreSQL's snowball stemmer library, implementing step 6 of the stemming process with complex multi-branch pattern matching and conditional suffix transformations.

## Definition
```c
static int r_steps6(struct SN_env * z)
```

## Detailed Description
This function implements step 6 of the Greek stemming algorithm through a complex three-phase pattern matching and transformation process:

1. **Initial Phase**: Sets ket position and searches for patterns from array a_14 (6 patterns). If a match is found, the matched slice is deleted and counter I[0] is reset to 0.

2. **First Alternative Branch**: Attempts to match patterns with specific character constraints:
   - Checks for character 181 (µ) at position c-1
   - Searches patterns from array a_12 (7 patterns)
   - Provides 2 replacement options based on among_var:
     - Case 1: 6-character replacement (s_45)
     - Case 2: 2-character replacement (s_46)

3. **Second Alternative Branch** (fallback): If first branch fails:
   - Checks for characters 186 (º) or 189 (½) at position c-1
   - Uses more extensive pattern matching from array a_13 (10 patterns)
   - Provides 10 different replacement options with varying string lengths (6-16 characters):
     - Cases 1-10: Different predefined string replacements (s_47 through s_56)

The function uses goto statements for control flow and includes comprehensive bounds checking.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing:
  - `ket`: End position marker for pattern matching
  - `bra`: Start position marker for pattern matching
  - `c`: Current cursor position
  - `l`: Length/limit of the string
  - `lb`: Lower bound for matching
  - `p`: Pointer to the string buffer
  - `I[0]`: Integer counter array (element 0 reset to 0)
- `among_var`: Local variable storing pattern matching results to determine replacement type
- `m1`: Local variable for position backtracking

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward pattern matching)
  - [slice_del](../s/slice_del.md) (slice deletion)
  - [slice_from_s](../s/slice_from_s.md) (slice replacement with predefined strings)
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md) (main stemming function)

## Notes and Other Information
- This is a static function, only accessible within the Greek stemmer module
- Uses predefined pattern arrays (a_12, a_13, a_14) and replacement strings (s_45 through s_56)
- Returns 1 on success, 0 on no match, or negative values on error
- The most complex step function with 12 possible replacement outcomes
- Uses character-specific filtering (181, 186, 189) for Unicode Greek characters
- Includes comprehensive bounds checking (z->c - 3 <= z->lb, z->c - 9 <= z->lb)
- Uses goto statements for efficient control flow between alternative matching strategies
- Part of the sequential stemming process, handling more complex morphological patterns

## Simplified Source

```c
static int r_steps6(struct SN_env * z) {
    // Initial phase: Find and delete suffix patterns from array a_14
    z->ket = z->c;
    if (!(find_among_b(z, a_14, 6))) return 0;
    z->bra = z->c;
    slice_del(z);  // Delete matched suffix
    z->I[0] = 0;   // Reset counter

    // Save position for potential backtracking
    int saved_pos = z->l - z->c;

    // Try first alternative: patterns requiring character µ (181)
    z->ket = z->c;
    z->bra = z->c;
    if (z->c - 3 > z->lb && z->p[z->c - 1] == 181) {  // Check for µ
        int pattern_type = find_among_b(z, a_12, 7);
        if (pattern_type && z->c <= z->lb) {
            switch (pattern_type) {
                case 1: slice_from_s(z, 6, s_45); break;
                case 2: slice_from_s(z, 2, s_46); break;
            }
            return 1;  // Success with first alternative
        }
    }

    // Fallback: patterns requiring characters º (186) or ½ (189)
    z->c = z->l - saved_pos;  // Restore position
    z->ket = z->c;
    if (z->c - 9 <= z->lb) return 0;  // Bounds check

    char last_char = z->p[z->c - 1];
    if (last_char != 186 && last_char != 189) return 0;  // Check for º or ½

    int pattern_type = find_among_b(z, a_13, 10);
    if (!pattern_type) return 0;
    z->bra = z->c;

    // Apply replacement based on pattern (10 different options)
    const char* replacements[] = {NULL, s_47, s_48, s_49, s_50, s_51,
                                  s_52, s_53, s_54, s_55, s_56};
    const int lengths[] = {0, 12, 8, 10, 6, 12, 10, 6, 16, 12, 10};

    slice_from_s(z, lengths[pattern_type], replacements[pattern_type]);
    return 1;  // Success
}
```