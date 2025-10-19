# r_remove_um

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_tamil.c:1124-1147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_tamil.c#L1124-L1147)

## Overview
Removes the specific Tamil "um" suffix pattern and replaces it with a standardized form while applying morphological corrections.

## Definition

```c
}

static int r_remove_um(struct SN_env * z)
```
## Detailed Description
This function handles a specific Tamil morphological pattern involving the "um" suffix. The function:

1. Validates minimum word length using r_has_min_length to prevent over-stemming
2. Sets up backward processing from the word end
3. Looks for a specific 9-character pattern (s_54) ending the word using exact backward matching
4. Replaces the matched pattern with a 3-character standardized form (s_55)
5. Applies morphological correction through r_fix_ending to ensure proper word formation
6. Uses cursor position preservation to maintain processing state

Unlike other suffix functions that may handle multiple patterns, this function targets one very specific morphological transformation.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing:
  - : State flag set to 1 when the "um" pattern is successfully processed
  - //: Cursor positions for boundary/current/limit management
  - /: Bracket positions marking the pattern for replacement
  - : Temporary cursor position for state preservation during post-processing

## Dependencies
- Functions called/Symbols referenced:
  - [r_has_min_length](r_has_min_length.md) (validates minimum word length)
  - [eq_s_b](../e/eq_s_b.md) (exact backward string matching for 9-character pattern s_54)
  - [slice_from_s](../s/slice_from_s.md) (replaces matched pattern with 3-character form s_55)
  - [r_fix_ending](r_fix_ending.md) (applies morphological corrections after replacement)
- Called from (representative examples):
  - [tamil_UTF_8_stem](../t/tamil_UTF_8_stem.md) (main Tamil stemming function)

## Notes and Other Information
- Handles a very specific Tamil morphological pattern ("um" suffix variations)
- More targeted than other suffix removal functions, focusing on one 9-character pattern
- Includes post-processing correction through r_fix_ending, indicating this transformation may affect word structure
- The pattern replacement (9 chars → 3 chars) suggests significant morphological simplification
- Part of the Tamil stemming pipeline that handles various morphological endings and particles
- The specific pattern (s_54/s_55) likely represents a common Tamil grammatical construction that needs normalization for search/indexing purposes

## Simplified Source

```c
static int r_remove_um(struct SN_env * z) {
    // Initialize state flag
    z->I[1] = 0;

    // Check minimum word length before processing
    int ret = r_has_min_length(z);
    if (ret <= 0) return ret;

    // Set up backward processing boundaries
    z->lb = z->c;
    z->c = z->l;

    // Look for specific 9-character "um" pattern
    z->ket = z->c;
    if (!eq_s_b(z, 9, s_54)) {
        return 0; // Pattern not found
    }

    // Replace 9-character pattern with 3-character standardized form
    z->bra = z->c;
    slice_from_s(z, 3, s_55);

    // Mark successful processing
    z->I[1] = 1;

    // Reset to beginning and apply morphological corrections
    z->c = z->lb;
    int saved_position = z->c;
    r_fix_ending(z);
    z->c = saved_position; // Restore position after corrections

    return 1; // Success
}
```