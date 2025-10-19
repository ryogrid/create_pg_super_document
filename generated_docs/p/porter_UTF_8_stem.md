# porter_UTF_8_stem

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_porter.c:564-719](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_porter.c#L564-L719)

## Overview
The porter_UTF_8_stem function is the main entry point for the Porter stemming algorithm implementation for UTF-8 encoded text, executing the complete sequence of stemming steps to reduce words to their morphological roots.

## Definition

```c
}

extern int porter_UTF_8_stem(struct SN_env * z)
```
## Detailed Description
This function implements the complete Porter stemming algorithm for UTF-8 encoded text. The Porter stemmer is a widely-used algorithm for reducing English words to their stems by removing common morphological and inflectional endings. The function operates in several phases:

1. **Preprocessing Phase**: Converts initial 'y' to 'Y' and marks vowel-adjacent 'y' characters as 'Y' to handle them as consonants during processing
2. **Region Identification**: Establishes R1 and R2 regions which define morphological boundaries used by subsequent steps
3. **Sequential Step Execution**: Applies the five main Porter algorithm steps:
   - Step 1a: Handle plurals and past participles
   - Step 1b: Handle verb forms and double consonants
   - Step 1c: Replace terminal 'y' with 'i'
   - Step 2: Remove derivational suffixes
   - Step 3: Remove additional derivational suffixes
   - Step 4: Remove common suffixes in R2 region
   - Step 5a: Remove terminal 'e'
   - Step 5b: Remove double 'l'
4. **Postprocessing**: Convert any remaining 'Y' characters back to 'y'

The algorithm ensures proper morphological analysis by respecting region boundaries and applying rules in the correct sequence.

## Parameters / Member Variables
- `*z`: Pointer to the SN_env stemming environment structure containing:
## Dependencies
- Functions called/Symbols referenced:
  - [in_grouping_U](../i/in_grouping_U.md) (tests character group membership for UTF-8)
  - [out_grouping_U](../o/out_grouping_U.md) (finds characters outside a group for UTF-8)
  - [skip_utf8](../s/skip_utf8.md) (advances cursor by UTF-8 characters)
  - [slice_from_s](../s/slice_from_s.md) (replaces text slice with string)
  - [r_Step_1a](../r/r_Step_1a.md), r_Step_1b, r_Step_1c (Step 1 implementations)
  - [r_Step_2](../r/r_Step_2.md), r_Step_3, r_Step_4 (Steps 2-4 implementations)
  - [r_Step_5a](../r/r_Step_5a.md), r_Step_5b (Step 5 implementations)
- Called from:
  - External stemming interfaces (not shown in current symbol database)

## Notes and Other Information
- Returns 1 on successful completion of the stemming process
- This is the UTF-8 variant of the Porter stemmer, handling multi-byte characters properly
- The function modifies the input word in-place within the SN_env structure
- Uses integer variables I[0], I[1], I[2] for region boundaries and state tracking
- Part of the Snowball stemming library integrated into PostgreSQL for text search functionality
- File location: src/backend/snowball/libstemmer/stem_UTF_8_porter.c:564-719

## Simplified Source

```c
extern int porter_UTF_8_stem(struct SN_env * z) {
    // Initialize state tracking
    z->I[2] = 0;

    // Step 1: Convert initial 'y' to 'Y' if present
    if (z->c < z->l && z->p[z->c] == 'y') {
        slice_from_s(z, 1, s_21);  // Replace 'y' with 'Y'
        z->I[2] = 1;  // Mark Y conversion occurred
    }

    // Step 2: Convert vowel-adjacent 'y' to 'Y' throughout word
    while (/* find vowel followed by 'y' */) {
        slice_from_s(z, 1, s_22);  // Replace 'y' with 'Y'
        z->I[2] = 1;
    }

    // Step 3: Identify R1 and R2 regions for morphological boundaries
    z->I[1] = z->l;  // R1 start (default to end)
    z->I[0] = z->l;  // R2 start (default to end)

    // Find first vowel-consonant boundary for R1
    // Find second vowel-consonant boundary for R2
    find_regions(z);

    // Step 4: Apply Porter algorithm steps sequentially
    z->lb = z->c;
    z->c = z->l;  // Start from end of word

    // Execute the main Porter stemming steps
    r_Step_1a(z);  // Handle plurals, past participles
    r_Step_1b(z);  // Handle verb forms, double consonants
    r_Step_1c(z);  // Replace terminal 'y' with 'i'
    r_Step_2(z);   // Remove derivational suffixes
    r_Step_3(z);   // Remove additional suffixes
    r_Step_4(z);   // Remove common suffixes in R2
    r_Step_5a(z);  // Remove terminal 'e'
    r_Step_5b(z);  // Remove double 'l'

    // Step 5: Convert remaining 'Y' back to 'y' if any conversions occurred
    z->c = z->lb;
    if (z->I[2]) {  // If Y conversions were made
        while (/* find 'Y' characters */) {
            slice_from_s(z, 1, s_23);  // Replace 'Y' with 'y'
        }
    }

    return 1;  // Success
}
```