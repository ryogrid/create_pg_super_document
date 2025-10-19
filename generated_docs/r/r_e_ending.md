# r_e_ending

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_dutch.c:345-367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_dutch.c#L345-L367)

## Overview
r_e_ending is a specialized function in the Dutch Snowball stemming algorithm that handles the removal of trailing 'e' endings from words with specific vowel pattern requirements.

## Definition

```c
}

static int r_e_ending(struct SN_env * z)
```
## Detailed Description
The r_e_ending function implements a sophisticated rule for Dutch stemming that removes the trailing 'e' character from words when specific conditions are met. The function performs a multi-step validation process:

1. **Initialization**: Sets z->I[2] to 0 as a state indicator
2. **E Detection**: Checks if the current word ends with the character 'e'
3. **Boundary Validation**: Ensures the 'e' removal would occur within the R1 region using r_R1
4. **Vowel Pattern Check**: Uses out_grouping_b to verify that the character preceding 'e' is NOT a vowel (group g_v, range 97-232)
5. **Removal**: Deletes the 'e' character using slice_del
6. **State Update**: Sets z->I[2] to 1 to indicate successful e-ending removal
7. **Consonant Cleanup**: Calls r_undouble to handle any doubled consonants that may result from the removal

This function ensures that 'e' endings are only removed when they follow consonants, preventing over-stemming that could occur with vowel-e patterns.

## Parameters / Member Variables
- `*z`: Pointer to SN_env structure containing the stemming environment with cursor positions, boundaries, state indicators, and character data
## Dependencies
- Functions called/Symbols referenced:
  - [r_R1](r_R1.md): Validates that the current position is within the R1 region
  - [out_grouping_b](../o/out_grouping_b.md): Checks if character is NOT in specified vowel group
  - [slice_del](../s/slice_del.md): Removes character sequence from the word
  - [r_undouble](r_undouble.md): Removes doubled consonants after suffix removal
- Called from (representative examples):
  - [r_standard_suffix](r_standard_suffix.md): Dutch standard suffix processing (multiple locations)

## Notes and Other Information
- The function uses z->I[2] as a state flag to track successful e-ending removal for downstream processing
- The vowel group check (g_v, 97-232) covers the Dutch vowel character set including accented characters
- This function is specifically designed for Dutch morphology and vowel-consonant patterns
- The integration with r_undouble ensures proper consonant handling after suffix removal
- Only called from r_standard_suffix, indicating its role as a specialized sub-operation in Dutch stemming
- Available in both ISO-8859-1 and UTF-8 variants for different character encodings

## Simplified Source

```c
static int r_e_ending(struct SN_env * z) {
    // Initialize state flag
    z->I[2] = 0;
    z->ket = z->c;

    // Check if word ends with 'e'
    if (z->c <= z->lb || z->p[z->c - 1] != 'e') {
        return 0;  // No 'e' ending found
    }

    // Move cursor to position before 'e'
    z->c--;
    z->bra = z->c;

    // Verify we're in the R1 region (valid stemming area)
    if (r_R1(z) <= 0) {
        return 0;  // Not in R1 region
    }

    // Check that character before 'e' is NOT a vowel
    int saved_pos = z->l - z->c;
    if (out_grouping_b(z, g_v, 97, 232, 0)) {
        return 0;  // Previous character is a vowel, don't remove 'e'
    }
    z->c = z->l - saved_pos;  // Restore position

    // Remove the 'e' ending
    slice_del(z);

    // Set success flag
    z->I[2] = 1;

    // Clean up any doubled consonants that may result
    r_undouble(z);

    return 1;  // Success
}
```

This function handles Dutch 'e' ending removal with these conditions:
1. **Word ends with 'e'**: Checks for trailing 'e' character
2. **R1 region check**: Ensures removal occurs in morphologically appropriate area
3. **Consonant requirement**: Only removes 'e' if preceded by a consonant (not vowel)
4. **Safe removal**: Deletes the 'e' and sets success flag (z->I[2] = 1)
5. **Consonant cleanup**: Calls r_undouble to fix doubled consonants

This prevents over-stemming by preserving 'e' endings after vowels, which are often part of the word root in Dutch.