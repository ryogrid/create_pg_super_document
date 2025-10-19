# r_Step_1b

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c:537-611](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c#L537-L611)

## Overview
The r_Step_1b function implements Step 1b of the English Porter stemming algorithm, handling the removal and transformation of specific verb and adverb suffixes (ed, eed, ing, edly, eedly, ingly).

## Definition
```c
static int r_Step_1b(struct SN_env * z)
```

## Detailed Description
This function performs Step 1b of the English stemming algorithm, which deals with past tense and gerund forms. It operates in two main cases:

**Case 1** (eed/eedly suffixes): 
- Only removes the suffix if it occurs in the R1 region
- Replaces with "ee"

**Case 2** (ed/ing/edly/ingly suffixes):
- Removes the suffix only if the stem contains a vowel
- After removal, applies additional transformations based on the resulting stem:
  - **Subcase 1**: If stem ends with specific patterns (bl), adds "e"
  - **Subcase 2**: If stem ends with doubled consonants (bb, dd, ff, gg, mm, nn, pp, rr, tt), removes one letter
  - **Subcase 3**: If at R1 boundary and stem is "short", adds "e"

The function uses bit manipulation for efficient character class checking and employs the shortv test to determine if a stem is "short" (contains a short syllable at the end).

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemming environment with:
  - : Current cursor position
  - : Length of the string  
  - : Left boundary limit
  - : Character array being processed
  - : End marker for current suffix
  - : Start marker for current suffix
  - : R1 region boundary position

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (searches backwards for matching suffixes)
  - [r_R1](r_R1.md) (tests if position is in R1 region)
  - [slice_from_s](../s/slice_from_s.md) (replaces marked substring with specified string)
  - [slice_del](../s/slice_del.md) (deletes the marked substring)
  - [out_grouping_b](../o/out_grouping_b.md) (checks if character is outside specified group)
  - [r_shortv](r_shortv.md) (tests if word ends with a short syllable)
  - [insert_s](../i/insert_s.md) (inserts string at specified position)
  - a_4 (array of suffixes: ed, eed, ing, edly, eedly, ingly)
  - a_3 (array for post-processing patterns)
  - s_5, s_6, s_7 (replacement strings: "ee", "e", "e")
  - g_v (vowel character group)
- Called from (representative examples):
  - [english_ISO_8859_1_stem](../e/english_ISO_8859_1_stem.md)
  - [porter_ISO_8859_1_stem](../p/porter_ISO_8859_1_stem.md)
  - [english_UTF_8_stem](../e/english_UTF_8_stem.md)  
  - [porter_UTF_8_stem](../p/porter_UTF_8_stem.md)

## Notes and Other Information
- Returns 1 on successful completion, 0 if no changes were made, or negative values on error
- Uses sophisticated bit manipulation (33554576 and 68514004 bit masks) for efficient character classification
- The vowel test ensures that suffixes are only removed from valid verb forms (those containing vowels)
- Post-processing rules handle common English morphological patterns like doubled consonants
- Critical component of Porter stemming algorithm for handling English verb forms in PostgreSQL's full-text search

## Simplified Source

```c
static int r_Step_1b(struct SN_env * z) {
    // Handle verb suffixes: ed, eed, ing, edly, eedly, ingly
    z->ket = z->c;

    // Check if word ends with target suffixes (using bit mask optimization)
    if (z->c - 1 <= z->lb ||
        z->p[z->c - 1] >> 5 != 3 ||
        !((33554576 >> (z->p[z->c - 1] & 0x1f)) & 1)) {
        return 0;
    }

    int among_var = find_among_b(z, a_4, 6);  // Find suffix pattern
    if (!among_var) return 0;

    z->bra = z->c;
    switch (among_var) {
        case 1:  // eed/eedly -> ee (only in R1 region)
            if (r_R1(z) <= 0) return 0;  // Must be in R1
            slice_from_s(z, 2, s_5);     // Replace with "ee"
            break;

        case 2:  // ed/ing/edly/ingly -> delete (if stem contains vowel)
            // Test if stem contains a vowel
            int test_pos = z->l - z->c;
            if (out_grouping_b(z, g_v, 97, 121, 1) < 0) return 0;  // No vowel found
            z->c = z->l - test_pos;

            slice_del(z);  // Remove suffix

            // Post-processing: check what remains after deletion
            test_pos = z->l - z->c;
            among_var = find_among_b(z, a_3, 13);  // Check remaining pattern
            z->c = z->l - test_pos;

            // Apply post-processing rules
            switch (among_var) {
                case 1:  // Ends with bl -> add "e"
                    insert_s(z, z->c, z->c, 1, s_6);  // Insert "e"
                    break;

                case 2:  // Doubled consonant -> remove one
                    z->ket = z->c;
                    z->c--;
                    z->bra = z->c;
                    slice_del(z);  // Remove doubled consonant
                    break;

                case 3:  // At R1 boundary + short stem -> add "e"
                    if (z->c == z->I[1] && r_shortv(z) > 0) {
                        insert_s(z, z->c, z->c, 1, s_7);  // Insert "e"
                    }
                    break;
            }
            break;
    }

    return 1;  // Success
}
```