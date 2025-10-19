# catalan_UTF_8_stem

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_catalan.c:1399-1445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_catalan.c#L1399-L1445)

## Overview
catalan_UTF_8_stem is the main external entry point function that implements the complete Catalan Snowball stemming algorithm for UTF-8 encoded text, orchestrating all stemming phases in the proper sequence.

## Definition
```c
extern int catalan_UTF_8_stem(struct SN_env * z)
```

## Detailed Description
This function serves as the primary interface for Catalan word stemming, implementing a multi-phase stemming process that follows the Snowball algorithmic approach. The function operates through several distinct phases:

1. **Region Marking**: Calls r_mark_regions() to identify morphological boundaries (R1/R2 regions) within the word
2. **Setup**: Positions cursors at word boundaries (lb = c, c = l) to prepare for backward processing
3. **Pronoun Removal**: Attempts to remove attached pronouns using r_attached_pronoun()
4. **Primary Suffix Processing**: Tries standard suffix removal first via r_standard_suffix(), falling back to verb suffix processing via r_verb_suffix() if no standard suffixes match
5. **Residual Processing**: Handles remaining suffixes through r_residual_suffix()  
6. **Final Cleaning**: Applies character normalization and cleanup via r_cleaning()

The function uses careful cursor position management with backup/restore mechanisms (m1, m2, m3, m4 markers) to ensure proper processing flow and error recovery.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the UTF-8 text to be stemmed, along with cursor positions, region boundaries, and working buffers

## Dependencies
- Functions called/Symbols referenced:
  - [r_mark_regions](../r/r_mark_regions.md) (identifies morphological regions)
  - [r_attached_pronoun](../r/r_attached_pronoun.md) (removes attached pronouns)
  - [r_standard_suffix](../r/r_standard_suffix.md) (processes standard morphological suffixes)
  - [r_verb_suffix](../r/r_verb_suffix.md) (processes verbal suffixes)
  - [r_residual_suffix](../r/r_residual_suffix.md) (handles remaining suffixes)
  - [r_cleaning](../r/r_cleaning.md) (performs character normalization)
- Called from (representative examples):
  - No direct references found - likely called via function pointers or external stemming interfaces

## Notes and Other Information
- This is the UTF-8 variant of the Catalan stemmer, designed to handle Unicode text properly
- The function implements a fallback strategy: tries standard suffixes first, then verb suffixes if needed
- Uses goto labels (lab0, lab1, lab2) for efficient control flow in the suffix processing logic  
- Returns 1 on successful stemming completion, or negative values on errors
- The extern declaration indicates this is a public API function intended for use by external modules
- Cursor position management is critical - the function carefully saves and restores positions to handle multiple processing attempts
- The algorithm follows the established Snowball pattern used across multiple language stemmers

## Simplified Source

```c
extern int catalan_UTF_8_stem(struct SN_env * z) {
    // Phase 1: Mark morphological regions (R1/R2 boundaries)
    int ret = r_mark_regions(z);
    if (ret < 0) return ret;

    // Phase 2: Setup cursors for backward processing
    z->lb = z->c;
    z->c = z->l;

    // Phase 3: Remove attached pronouns
    int pos1 = z->l - z->c;
    r_attached_pronoun(z);
    z->c = z->l - pos1;

    // Phase 4: Try standard suffixes, fall back to verb suffixes
    int pos2 = z->l - z->c;
    int pos3 = z->l - z->c;

    if (r_standard_suffix(z) == 0) {
        // No standard suffix found, try verb suffix
        z->c = z->l - pos3;
        r_verb_suffix(z);
    }
    z->c = z->l - pos2;

    // Phase 5: Handle residual suffixes
    int pos4 = z->l - z->c;
    r_residual_suffix(z);
    z->c = z->l - pos4;

    // Phase 6: Final character cleaning
    z->c = z->lb;
    int saved_pos = z->c;
    r_cleaning(z);
    z->c = saved_pos;

    return 1;  // Success
}
```