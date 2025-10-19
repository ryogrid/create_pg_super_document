# r_t_plural

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_finnish.c:527-572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_finnish.c#L527-L572)

## Overview
The r_t_plural function identifies and removes Finnish plural endings that involve the letter t followed by vowel-containing patterns in the stemming process.

## Definition
static int r_t_plural(struct SN_env * z)

## Detailed Description
This function handles the removal of Finnish plural markers that contain the character t followed by specific vowel patterns. It operates in two distinct phases with different morphological boundaries and implements a complex validation process.

The function follows this two-phase process:

**Phase 1: t-ending removal (R2 region)**
1. Sets up processing boundaries using z->I[1] (likely the R2 region) as the left limit
2. Checks if the character immediately before the cursor is t
3. Moves the cursor back past the t
4. Validates that the character before t is a vowel (using g_V1 group, ASCII 97-246)
5. If validation passes, removes the t using slice_del

**Phase 2: Vowel-ending pattern matching (R1 region)**
1. Sets up new processing boundaries using z->I[0] (likely the R1 region) as the left limit
2. Checks if the character immediately before the cursor is a (ASCII 97)
3. Uses find_among_b to match against 2 predefined patterns (a_9 array)
4. For case 1, performs an exclusion check using eq_s_b with s_4 (a 2-character string)
5. If the exclusion check matches, returns without removal
6. Otherwise, removes the matched ending using slice_del

This two-phase approach ensures proper handling of complex t-plural forms that require both consonant and vowel validation.

## Parameters / Member Variables
- z: Pointer to SN_env structure containing string buffer, cursor positions, and processing state

## Dependencies
- Functions called/Symbols referenced:
  - [in_grouping_b](../i/in_grouping_b.md) (character group membership check for backward processing)
  - [find_among_b](../f/find_among_b.md) (pattern matching for backward processing)
  - [eq_s_b](../e/eq_s_b.md) (string equality check for backward processing)
  - [slice_del](../s/slice_del.md) (removes matched substring)
- Called from (representative examples):
  - [finnish_ISO_8859_1_stem](../f/finnish_ISO_8859_1_stem.md) (main stemming function)
  - [finnish_UTF_8_stem](../f/finnish_UTF_8_stem.md) (UTF-8 variant of stemming function)

## Notes and Other Information
- Part of the Finnish Snowball stemmer algorithm for t-based plural ending removal
- Uses dual-phase processing with different morphological boundary regions (R1 and R2)
- Phase 1 operates in R2 region for t removal with vowel validation
- Phase 2 operates in R1 region for a-ending pattern matching using a_9 array (2 patterns)
- Character code 97 corresponds to ASCII a, and 116 would be ASCII t
- Includes exclusion logic in case 1 to prevent removal of certain patterns (s_4)
- Returns 1 on successful processing, 0 on failure or exclusion
- Located in stem_ISO_8859_1_finnish.c indicating ISO 8859-1 character encoding support
- This function handles complex Finnish plural morphology involving consonant-vowel sequences

## Simplified Source

```c
static int r_t_plural(struct SN_env * z) {
    int among_var;

    // Phase 1: Remove 't' ending in R2 region with vowel validation
    {
        int mlimit1;
        if (z->c < z->I[1]) return 0;  // Check R2 boundary
        mlimit1 = z->lb; z->lb = z->I[1];

        z->ket = z->c;
        if (z->c <= z->lb || z->p[z->c - 1] != 't') {
            z->lb = mlimit1;
            return 0;
        }
        z->c--;  // Move past 't'
        z->bra = z->c;

        // Check if character before 't' is a vowel
        {
            int m_test = z->l - z->c;
            if (in_grouping_b(z, g_V1, 97, 246, 0)) {
                z->lb = mlimit1;
                return 0;
            }
            z->c = z->l - m_test;
        }

        // Remove the 't'
        if (slice_del(z) < 0) return -1;
        z->lb = mlimit1;
    }

    // Phase 2: Match and remove vowel patterns in R1 region
    {
        int mlimit2;
        if (z->c < z->I[0]) return 0;  // Check R1 boundary
        mlimit2 = z->lb; z->lb = z->I[0];

        z->ket = z->c;
        if (z->c - 2 <= z->lb || z->p[z->c - 1] != 'a') {
            z->lb = mlimit2;
            return 0;
        }

        // Find matching pattern from predefined set
        among_var = find_among_b(z, a_9, 2);
        if (!among_var) {
            z->lb = mlimit2;
            return 0;
        }
        z->bra = z->c;
        z->lb = mlimit2;
    }

    // Handle matched pattern
    switch (among_var) {
        case 1:
            // Check exclusion condition
            {
                int m_test = z->l - z->c;
                if (eq_s_b(z, 2, s_4)) {
                    return 0;  // Don't remove if exclusion matches
                }
                z->c = z->l - m_test;
            }
            break;
    }

    // Remove the matched ending
    if (slice_del(z) < 0) return -1;
    return 1;
}
```