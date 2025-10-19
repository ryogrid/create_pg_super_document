# r_en_ending

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_dutch.c:368-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_dutch.c#L368-L390)

## Overview
r_en_ending is a specialized function in the Dutch Snowball stemming algorithm that handles the removal of 'en' suffix endings from words with specific vowel pattern and exclusion requirements.

## Definition

```c
}

static int r_en_ending(struct SN_env * z)
```
## Detailed Description
The r_en_ending function implements a complex rule for Dutch stemming that removes 'en' suffixes when specific morphological conditions are met. The function performs a multi-step validation process:

1. **Region Validation**: Uses r_R1 to ensure the current position is within the R1 region
2. **Vowel Pattern Check**: Uses out_grouping_b to verify that the character preceding the 'en' suffix is NOT a vowel (group g_v, range 97-232)
3. **Exclusion Check**: Uses eq_s_b to check for a specific 3-character exclusion pattern (s_10) that would prevent 'en' removal
4. **Conditional Removal**: If the exclusion pattern is found, the function returns 0 (no action); otherwise, it proceeds
5. **Suffix Deletion**: Removes the 'en' suffix using slice_del
6. **Consonant Cleanup**: Calls r_undouble to handle any doubled consonants that may result from the suffix removal

This function ensures that 'en' suffixes are only removed when they follow consonants and don't match specific exclusion patterns, preventing incorrect stemming in Dutch morphology.

## Parameters / Member Variables
- `*z`: Pointer to SN_env structure containing the stemming environment with cursor positions, boundaries, and character data
## Dependencies
- Functions called/Symbols referenced:
  - [r_R1](r_R1.md): Validates that the current position is within the R1 region
  - [out_grouping_b](../o/out_grouping_b.md): Checks if character is NOT in specified vowel group
  - [eq_s_b](../e/eq_s_b.md): Checks for specific string pattern match (3-character exclusion pattern s_10)
  - [slice_del](../s/slice_del.md): Removes character sequence from the word
  - [r_undouble](r_undouble.md): Removes doubled consonants after suffix removal
- Called from (representative examples):
  - [r_standard_suffix](r_standard_suffix.md): Dutch standard suffix processing (multiple locations)

## Notes and Other Information
- The function includes a specific exclusion mechanism using s_10 pattern matching to prevent incorrect 'en' suffix removal
- The vowel group check (g_v, 97-232) covers the Dutch vowel character set including accented characters
- Uses the goto/label mechanism (lab0) for control flow in the exclusion pattern check
- The integration with r_undouble ensures proper consonant doubling cleanup after suffix removal
- Only called from r_standard_suffix, indicating its role as a specialized sub-operation in Dutch stemming
- Available in both ISO-8859-1 and UTF-8 variants for different character encodings
- The m1 and m2 variables are used for position tracking with explicit void casting to suppress unused variable warnings

## Simplified Source

```c
static int r_en_ending(struct SN_env * z) {
    // Verify we're in the R1 region (valid stemming area)
    if (r_R1(z) <= 0) {
        return 0;  // Not in R1 region
    }

    // Check that character before 'en' is NOT a vowel
    int saved_pos1 = z->l - z->c;
    if (out_grouping_b(z, g_v, 97, 232, 0)) {
        return 0;  // Previous character is a vowel, don't remove 'en'
    }
    z->c = z->l - saved_pos1;  // Restore position

    // Check for exclusion pattern (s_10) that prevents 'en' removal
    int saved_pos2 = z->l - z->c;
    if (eq_s_b(z, 3, s_10)) {
        return 0;  // Exclusion pattern found, don't remove 'en'
    }
    z->c = z->l - saved_pos2;  // Restore position

    // Remove the 'en' suffix
    slice_del(z);

    // Clean up any doubled consonants that may result
    r_undouble(z);

    return 1;  // Success
}
```

This function handles Dutch 'en' ending removal with these conditions:
1. **R1 region check**: Ensures removal occurs in morphologically appropriate area
2. **Consonant requirement**: Only removes 'en' if preceded by a consonant (not vowel)
3. **Exclusion pattern**: Checks for specific 3-character pattern (s_10) that prevents removal
4. **Safe removal**: Deletes the 'en' suffix if all conditions are met
5. **Consonant cleanup**: Calls r_undouble to fix doubled consonants

This prevents over-stemming by:
- Preserving 'en' endings after vowels
- Avoiding removal when specific exclusion patterns are present
- Maintaining Dutch morphological integrity