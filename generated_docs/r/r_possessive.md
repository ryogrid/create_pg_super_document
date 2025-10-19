# r_possessive

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_finnish.c:332-397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_finnish.c#L332-L397)

## Overview
The r_possessive function removes Finnish possessive suffixes from words during the stemming process, handling the complex morphological transformations required for Finnish possessive markers.

## Definition
static int r_possessive(struct SN_env * z)

## Detailed Description
This function implements the Finnish possessive suffix removal step in the Snowball stemming algorithm. Finnish has a rich possessive system with suffixes that indicate the person and number of the possessor (my, your, his/her, our, your plural, their). The function identifies and removes these possessive markers while applying necessary morphological transformations.

The algorithm operates within the R1 region boundary and uses a multi-case switch structure to handle different types of possessive suffixes:

- **Case 1**: Handles possessive suffixes that should not be removed if preceded by 'k'
- **Case 2**: Removes possessive and replaces a specific pattern (s_0) with another (s_1)
- **Case 3**: Simple possessive suffix removal
- **Cases 4-6**: Handle possessive suffixes with specific vowel requirements:
  - Case 4: Requires preceding vowel 'a' (character 97)
  - Case 5: Requires preceding vowel 'ä' (character 228)
  - Case 6: Requires preceding vowel 'e' (character 101) with additional pattern matching

Each case performs pattern matching against different suffix arrays (a_1, a_2, a_3, a_4) to ensure accurate identification of possessive markers.

## Parameters / Member Variables
- : Pointer to SN_env structure containing:
  - : Current cursor position
  - : R1 region boundary marker
  - : Left boundary for processing
  - : End position of matched substring
  - : Start position of matched substring
  - : Length of the string
  - : Pointer to the string data
- : Local variable storing the type of possessive suffix found (1-6)
- : Local variable storing the original left boundary
- : Local variable for position tracking

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward pattern matching function
  - [eq_s_b](../e/eq_s_b.md): Backward string equality test
  - [slice_del](../s/slice_del.md): Function to delete matched substring
  - [slice_from_s](../s/slice_from_s.md): Function to replace substring with new content
  - a_1, a_2, a_3, a_4: Arrays containing possessive suffix patterns
  - s_0, s_1: String constants for pattern replacement
- Called from (representative examples):
  - [finnish_ISO_8859_1_stem](../f/finnish_ISO_8859_1_stem.md): Main Finnish stemming function
  - [finnish_UTF_8_stem](../f/finnish_UTF_8_stem.md): UTF-8 version of Finnish stemming

## Notes and Other Information
This function is highly specific to Finnish morphology, reflecting the complex possessive system of the Finnish language. Finnish possessive suffixes can trigger vowel harmony changes and consonant gradation, which is why the function includes multiple cases with vowel-specific requirements. The function handles both simple removal and morphological transformations (case 2 with replacement). The character codes used (97='a', 228='ä', 101='e') reflect the ISO-8859-1 encoding for Finnish vowels. This function typically runs after particle removal but before case suffix processing in the Finnish stemming pipeline.

## Simplified Source

```c
static int r_possessive(struct SN_env * z) {
    int possessive_type;

    // Set boundaries to R1 region for possessive processing
    if (z->c < z->I[1]) return 0;
    int saved_boundary = z->lb;
    z->lb = z->I[1];

    // Find possessive suffix from predefined list
    z->ket = z->c;
    possessive_type = find_among_b(z, a_4, 9);
    if (!possessive_type) {
        z->lb = saved_boundary;
        return 0;
    }
    z->bra = z->c;
    z->lb = saved_boundary;

    // Apply removal rules based on possessive type
    switch (possessive_type) {
        case 1:
            // Don't remove if preceded by 'k'
            if (z->c > z->lb && z->p[z->c - 1] == 'k') {
                return 0;
            }
            slice_del(z);
            break;

        case 2:
            // Remove possessive and replace specific pattern
            slice_del(z);
            z->ket = z->c;
            if (eq_s_b(z, 3, s_0)) {
                z->bra = z->c;
                slice_from_s(z, 3, s_1);
            }
            break;

        case 3:
            // Simple possessive removal
            slice_del(z);
            break;

        case 4:
            // Requires preceding 'a' vowel
            if (z->c <= z->lb || z->p[z->c - 1] != 'a') return 0;
            if (find_among_b(z, a_1, 6)) slice_del(z);
            break;

        case 5:
            // Requires preceding 'ä' vowel
            if (z->c <= z->lb || z->p[z->c - 1] != 228) return 0;
            if (find_among_b(z, a_2, 6)) slice_del(z);
            break;

        case 6:
            // Requires preceding 'e' vowel
            if (z->c <= z->lb || z->p[z->c - 1] != 'e') return 0;
            if (find_among_b(z, a_3, 2)) slice_del(z);
            break;
    }
    return 1;
}
```