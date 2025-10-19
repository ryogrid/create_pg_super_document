# r_step5b

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3044-3093](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L3044-L3093)

## Overview
A static function in the Greek stemmer that performs step 5b of the Greek stemming algorithm, handling specific morphological patterns and vowel transformations in Greek words.

## Definition

```c
}

static int r_step5b(struct SN_env * z)
```
## Detailed Description
The r_step5b function is part of the Greek language stemming algorithm implementation in PostgreSQL's snowball stemmer library. This function performs step 5b of the Greek stemming process, which involves:

1. **Pattern Matching and Deletion**: First attempts to find and remove specific Greek morphological patterns using the a_38 lookup table (11 entries)
2. **Secondary Pattern Processing**: After initial deletion, looks for additional patterns using the a_37 lookup table (2 entries) and performs substitution with specific Greek characters
3. **Vowel-based Processing**: Handles Greek words ending in specific patterns, checking for vowel groups and performing appropriate transformations
4. **Final Pattern Matching**: Uses the a_39 lookup table (95 entries) for comprehensive pattern matching and applies final transformations

The function uses backward searching (indicated by the '_b' suffix in helper functions) to process Greek morphological endings from right to left, which is typical for suffix-based stemming operations.

## Parameters / Member Variables
- : Pointer to SN_env structure containing:
  - : Current cursor position in the string
  - : Length of the string being processed  
  - : Left boundary for processing
  - : Pointer to the string buffer
  - : End position marker for substring operations
  - : Start position marker for substring operations
  - : Integer array for storing intermediate results

## Dependencies
- Functions called/Symbols referenced:
  - : Backward pattern matching function
  - : Function to delete a substring slice
  - : Function to replace slice with specific string
  - : Backward string equality check function
  - : Backward Unicode character grouping check function
- Called from (representative examples):
  - : Main Greek stemming function

## Notes and Other Information
- This function is specific to Greek language morphology and handles UTF-8 encoded Greek text
- The function uses multiple lookup tables (a_37, a_38, a_39) that contain Greek morphological patterns
- Returns 1 on successful completion, 0 if no patterns matched, or negative values on error
- Part of a larger stemming pipeline that processes Greek words through multiple sequential steps
- The function handles complex Greek vowel patterns and morphological transformations specific to Modern Greek

## Simplified Source

```c
static int r_step5b(struct SN_env * z) {
    // Phase 1: Optional complex pattern matching with backtracking
    int saved_pos1 = z->l - z->c;
    z->ket = z->c;

    // Check for specific character (181) and minimum length
    if (z->c - 9 > z->lb && z->p[z->c - 1] == 181) {
        if (find_among_b(z, a_38, 11)) {  // Find from a_38 (11 patterns)
            z->bra = z->c;
            slice_del(z);  // Remove matched pattern
            z->I[0] = 0;   // Reset state

            // Secondary pattern check with character validation
            z->ket = z->c;
            z->bra = z->c;
            if (z->c - 3 > z->lb && (z->p[z->c - 1] == 129 || z->p[z->c - 1] == 131)) {
                if (find_among_b(z, a_37, 2) && z->c <= z->lb) {  // a_37 has 2 patterns
                    slice_from_s(z, 8, s_76);  // Replace with specific string
                }
            }
        }
    }
    z->c = z->l - saved_pos1;  // Restore position

    // Phase 2: Mandatory suffix processing
    z->ket = z->c;
    if (!eq_s_b(z, 6, s_77)) return 0;  // Must find specific 6-char suffix
    z->bra = z->c;
    slice_del(z);  // Remove the suffix
    z->I[0] = 0;   // Reset state

    // Phase 3: Vowel-based replacement with backtracking
    int saved_pos2 = z->l - z->c;
    z->ket = z->c;
    z->bra = z->c;

    // If current character is in vowel group g_v2, replace with s_78
    if (!in_grouping_b_U(z, g_v2, 945, 969, 0)) {
        slice_from_s(z, 4, s_78);
    } else {
        // Restore position if no vowel found
        z->c = z->l - saved_pos2;
        z->ket = z->c;
    }

    // Phase 4: Final pattern matching at word boundary
    z->bra = z->c;
    if (!find_among_b(z, a_39, 95)) return 0;  // Find from a_39 (95 patterns)
    if (z->c > z->lb) return 0;  // Must be at word beginning

    // Final replacement
    slice_from_s(z, 4, s_79);

    return 1;  // Success
}
```