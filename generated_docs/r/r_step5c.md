# r_step5c

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3094-3144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L3094-L3144)

## Overview
A static function in the Greek stemmer that performs step 5c of the Greek stemming algorithm, handling specific morphological patterns and conditional vowel-based transformations in Greek words.

## Definition
```c
static int r_step5c(struct SN_env * z)
```

## Detailed Description
The r_step5c function is part of the Greek language stemming algorithm implementation in PostgreSQL's snowball stemmer library. This function performs step 5c of the Greek stemming process, which involves:

1. **Optional Pattern Removal**: Attempts to find and remove specific Greek morphological patterns using the a_40 lookup table (1 entry) if certain conditions are met
2. **Main Pattern Processing**: Looks for and removes a specific 6-character pattern (s_80) 
3. **Conditional Vowel Processing**: Based on vowel group analysis, performs different transformations:
   - If vowels are found in the g_v2 group (Greek vowels 945-969), applies s_81 transformation
   - Otherwise, uses a_41 lookup table (31 entries) for pattern matching and applies s_82 transformation
4. **Final Pattern Application**: Uses the a_42 lookup table (25 entries) for final pattern matching and applies s_83 transformation

The function employs a branching logic structure with multiple conditional paths based on Greek morphological and phonological rules.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing:
  - `c`: Current cursor position in the string
  - `l`: Length of the string being processed  
  - `lb`: Left boundary for processing
  - `p`: Pointer to the string buffer
  - `ket`: End position marker for substring operations
  - `bra`: Start position marker for substring operations
  - `I[0]`: Integer array for storing intermediate results

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward pattern matching function
  - [slice_del](../s/slice_del.md): Function to delete a substring slice
  - [slice_from_s](../s/slice_from_s.md): Function to replace slice with specific string
  - [eq_s_b](../e/eq_s_b.md): Backward string equality check function
  - [in_grouping_b_U](../i/in_grouping_b_U.md): Backward Unicode character grouping check function
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md): Main Greek stemming function

## Notes and Other Information
- This function is specific to Greek language morphology and handles UTF-8 encoded Greek text
- Uses multiple lookup tables (a_40, a_41, a_42) containing Greek morphological patterns of varying sizes
- Implements conditional logic based on Greek vowel classification (g_v2 group covering Unicode range 945-969)
- Returns 1 on successful completion, 0 if required patterns don't match, or negative values on error
- Part of a sequential stemming pipeline where step 5c follows previous stemming steps
- The function handles complex branching logic for different Greek morphological contexts

## Simplified Source

```c
static int r_step5c(struct SN_env * z) {
    // Phase 1: Optional pattern removal with backtracking
    int saved_pos1 = z->l - z->c;
    z->ket = z->c;

    // Check for specific character (181) and minimum length
    if (z->c - 9 > z->lb && z->p[z->c - 1] == 181) {
        if (find_among_b(z, a_40, 1)) {  // Find from a_40 (1 pattern)
            z->bra = z->c;
            slice_del(z);  // Remove matched pattern
            z->I[0] = 0;   // Reset state
        }
    }
    z->c = z->l - saved_pos1;  // Restore position

    // Phase 2: Mandatory suffix processing
    z->ket = z->c;
    if (!eq_s_b(z, 6, s_80)) return 0;  // Must find specific 6-char suffix
    z->bra = z->c;
    slice_del(z);  // Remove the suffix
    z->I[0] = 0;   // Reset state

    // Phase 3: Conditional vowel-based processing with backtracking
    int saved_pos2 = z->l - z->c;
    z->ket = z->c;
    z->bra = z->c;

    if (!in_grouping_b_U(z, g_v2, 945, 969, 0)) {
        // Path A: Vowel found, replace with s_81
        slice_from_s(z, 4, s_81);
    } else {
        // Path B: No vowel, try alternative pattern matching
        z->c = z->l - saved_pos2;
        z->ket = z->c;
        z->bra = z->c;

        if (find_among_b(z, a_41, 31)) {  // Find from a_41 (31 patterns)
            slice_from_s(z, 4, s_82);
        } else {
            // Restore position if no pattern found
            z->c = z->l - saved_pos2;
            z->ket = z->c;
        }
    }

    // Phase 4: Final pattern matching at word boundary
    z->bra = z->c;
    if (!find_among_b(z, a_42, 25)) return 0;  // Find from a_42 (25 patterns)
    if (z->c > z->lb) return 0;  // Must be at word beginning

    // Final replacement
    slice_from_s(z, 4, s_83);

    return 1;  // Success
}
```