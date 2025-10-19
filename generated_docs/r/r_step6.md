# r_step6

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3429-3449](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L3429-L3449)

## Overview
A static function in the Greek stemmer that performs step 6 of the Greek language stemming algorithm, handling specific morphological transformations and conditional suffix deletion based on step counter state.

## Definition
```c
static int r_step6(struct SN_env * z)
```

## Detailed Description
This function performs step 6 of the Snowball Greek stemming algorithm, which operates in two phases:

1. **Optional transformation phase**: Attempts to find and replace patterns from the a_65 array (ματος, ματα, ματων) with "μα" (s_106). This transformation is optional and uses a backtrack mechanism - if it fails, the cursor position is restored.

2. **Conditional deletion phase**: Only proceeds if the step counter (I[0]) is set (non-zero), indicating that previous steps have been executed. It searches for patterns from the large a_66 array (84 different Greek suffixes and morphological endings) and deletes them if found.

The function uses a sophisticated backtracking mechanism to handle optional transformations gracefully.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing:
  - `ket`: End position marker for substring operations
  - `bra`: Start position marker for substring operations  
  - `c`: Current cursor position
  - `l`: String length
  - `I[0]`: Step counter/flag indicating if previous steps were executed

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward suffix matching)
  - [slice_from_s](../s/slice_from_s.md) (insert substring)
  - [slice_del](../s/slice_del.md) (delete substring)
- Arrays used:
  - a_65 (3 Greek patterns: ματος, ματα, ματων)
  - a_66 (84 diverse Greek morphological endings and suffixes)
  - s_106 (Greek replacement "μα")
- Called from:
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md) at src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3655

## Notes and Other Information
- Returns 1 on successful processing, 0 if no transformation was applied or if I[0] is not set
- The first phase uses optional backtracking (lab0/goto mechanism) to try transformations without commitment
- The second phase is conditional on I[0] being set, creating a dependency on previous stemming steps
- Array a_66 is the largest suffix array in the Greek stemmer with 84 different patterns
- This step handles final cleanup and regularization of Greek word forms after the main morphological transformations
- The step counter mechanism ensures proper ordering and prevents inappropriate transformations

## Simplified Source

```c
static int r_step6(struct SN_env * z) {
    // Phase 1: Optional transformation with backtracking
    int saved_pos = z->l - z->c;
    z->ket = z->c;
    if (find_among_b(z, a_65, 3)) {
        z->bra = z->c;
        slice_from_s(z, 4, s_106);  // Replace with "μα" (s_106)
    } else {
        z->c = z->l - saved_pos;    // Restore position if no match
    }

    // Phase 2: Conditional deletion based on step counter
    if (!z->I[0]) return 0;  // Only proceed if previous steps executed

    z->ket = z->c;
    if (!find_among_b(z, a_66, 84)) return 0;  // Large array of 84 patterns
    z->bra = z->c;
    slice_del(z);  // Delete matched suffix

    return 1;
}
```