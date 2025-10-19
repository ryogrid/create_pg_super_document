# r_Step_1a

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c:481-536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c#L481-L536)

## Overview
The r_Step_1a function implements Step 1a of the English Porter stemming algorithm in the Snowball stemmer, handling the removal of certain possessive and plural suffixes.

## Definition
```c
static int r_Step_1a(struct SN_env * z)
```

## Detailed Description
This function performs the first step of the English stemming algorithm, processing two types of suffix patterns:

1. **Possessive suffixes**: Removes apostrophes and possessive forms (' , 's, 's')
2. **Plural suffixes**: Handles various plural endings (ied, s, ies, sses, ss, us) with specific transformations

The function operates in two main phases:
- First phase: Attempts to match and remove possessive suffixes using the a_1 array
- Second phase: Matches plural suffixes using the a_2 array and applies appropriate transformations

For plural suffixes, it applies these rules:
- **Case 1** (sses): Transforms to "ss" 
- **Case 2** (ied/ies): Transforms to "i" if preceded by only one letter, otherwise to "ie"
- **Case 3** (s): Deletes the 's' only if preceded by a valid letter (not in vowel group) and followed by a vowel

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemming environment with:
  - : Current cursor position
  - : Length of the string
  - : Left boundary limit
  - : Character array being processed
  - : End marker for current suffix
  - : Start marker for current suffix

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (searches backwards for matching suffixes)
  - [slice_del](../s/slice_del.md) (deletes the marked substring)
  - [slice_from_s](../s/slice_from_s.md) (replaces marked substring with specified string)
  - [out_grouping_b](../o/out_grouping_b.md) (checks if character is outside specified group)
  - a_1 (array of possessive suffixes: ', 's', 's)
  - a_2 (array of plural suffixes: ied, s, ies, sses, ss, us)
  - s_2, s_3, s_4 (replacement strings: "ss", "i", "ie")
  - g_v (vowel character group for validation)
- Called from (representative examples):
  - [english_ISO_8859_1_stem](../e/english_ISO_8859_1_stem.md)
  - [porter_ISO_8859_1_stem](../p/porter_ISO_8859_1_stem.md)
  - [english_UTF_8_stem](../e/english_UTF_8_stem.md)
  - [porter_UTF_8_stem](../p/porter_UTF_8_stem.md)

## Notes and Other Information
- Returns 1 on successful completion, 0 if no changes were made, or negative values on error
- Uses backward searching (find_among_b) to match suffixes from the end of the word
- Implements sophisticated logic for 'ied/ies' handling based on word length
- The vowel check in case 3 ensures that 's' is only removed from valid plural forms
- Part of the standard Porter stemming algorithm implementation in PostgreSQL's full-text search capabilities

## Simplified Source

```c
static int r_Step_1a(struct SN_env * z) {
    // Phase 1: Remove possessive suffixes (' , 's, 's')
    int saved_pos = z->l - z->c;
    z->ket = z->c;

    // Check for apostrophe or 's' at end
    if (z->c > z->lb && (z->p[z->c - 1] == 39 || z->p[z->c - 1] == 115)) {
        if (find_among_b(z, a_1, 3)) {  // Match possessive patterns
            z->bra = z->c;
            slice_del(z);  // Remove possessive suffix
        }
    }

    // Phase 2: Handle plural suffixes (ied, s, ies, sses, ss, us)
    z->ket = z->c;

    // Must end with 'd' or 's' to be a plural
    if (z->c <= z->lb || (z->p[z->c - 1] != 100 && z->p[z->c - 1] != 115)) {
        return 0;
    }

    int among_var = find_among_b(z, a_2, 6);  // Match plural patterns
    if (!among_var) return 0;

    z->bra = z->c;
    switch (among_var) {
        case 1:  // sses -> ss
            slice_from_s(z, 2, s_2);  // Replace with "ss"
            break;

        case 2:  // ied/ies -> i or ie
            if (z->c >= z->lb + 2) {
                slice_from_s(z, 1, s_3);  // Replace with "i"
            } else {
                slice_from_s(z, 2, s_4);  // Replace with "ie"
            }
            break;

        case 3:  // s -> delete (if valid context)
            // Only delete 's' if preceded by valid consonant + vowel
            z->c--;
            if (out_grouping_b(z, g_v, 97, 121, 1) > 0) {
                slice_del(z);  // Delete the 's'
            }
            break;
    }

    return 1;  // Success
}
```