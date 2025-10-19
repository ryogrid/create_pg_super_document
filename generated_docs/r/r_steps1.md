# r_steps1

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2541-2569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L2541-L2569)

## Overview
A secondary stemming function that performs additional Greek suffix removal and stem cleanup, working as part of a multi-phase stemming pipeline after the initial suffix processing.

## Definition
```c
static int r_steps1(struct SN_env * z)
```

## Detailed Description
This function implements a two-phase suffix processing approach for Greek stemming. In the first phase, it identifies and removes suffixes using pattern array `a_3` (containing 14 patterns) via `slice_del()`, completely deleting the matched suffix rather than replacing it with an alternative form.

The second phase performs stem restoration by matching against pattern array `a_2` (containing 31 patterns) and applying one of two transformation cases. The function includes a boundary check (`z->c > z->lb`) to ensure processing doesn't extend beyond the word boundaries. Case 1 replaces with a 2-byte sequence (`s_35`), while Case 2 uses a 4-byte sequence (`s_36`).

Like `r_step1`, this function resets the state flag `z->I[0] = 0` after the deletion phase, indicating coordination between stemming steps and state management for subsequent processing phases.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure (`struct SN_env *`) containing the word being processed, boundary markers, and processing state

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward pattern matching for suffix identification
  - [slice_del](../s/slice_del.md): Complete suffix removal function
  - [slice_from_s](../s/slice_from_s.md): String replacement using predefined constants
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md): Main Greek stemming function as part of the sequential stemming pipeline

## Notes and Other Information
- Represents an intermediate stemming phase that refines the results from `r_step1`
- The two-phase approach (deletion followed by restoration) handles complex Greek morphological patterns
- Pattern arrays `a_3` (14 patterns) and `a_2` (31 patterns) target different types of suffixes than `r_step1`
- Boundary checking ensures stemming operations remain within valid word limits
- The deletion-first approach suggests this handles suffixes that should be completely removed rather than transformed
- State management through `z->I[0]` reset maintains consistency across the multi-step stemming process
- Returns 1 on successful processing, 0 if no applicable patterns are found
- Located in src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2541-2569

## Simplified Source

```c
static int r_steps1(struct SN_env * z) {
    int pattern_match;

    // Phase 1: Delete suffixes using pattern array a_3 (14 patterns)
    z->ket = z->c;
    if (!find_among_b(z, a_3, 14)) return 0;
    z->bra = z->c;
    slice_del(z);  // Complete removal of matched suffix

    z->I[0] = 0;   // Reset state flag

    // Phase 2: Restore stem endings using pattern array a_2 (31 patterns)
    z->ket = z->c;
    z->bra = z->c;
    pattern_match = find_among_b(z, a_2, 31);
    if (!pattern_match) return 0;
    if (z->c > z->lb) return 0;  // Boundary check

    // Apply restoration based on pattern
    switch (pattern_match) {
        case 1: slice_from_s(z, 2, s_35); break;  // 2-byte replacement
        case 2: slice_from_s(z, 4, s_36); break;  // 4-byte replacement
    }

    return 1;  // Success
}
```