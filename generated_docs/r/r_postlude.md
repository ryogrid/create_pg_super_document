# r_postlude

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:2039-2066](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L2039-L2066)

## Overview
Performs final post-processing operations on Turkish words after stemming, including reserved word checking, vowel harmony restoration, and consonant transformations.

## Definition

```c
}

static int r_postlude(struct SN_env * z)
```
## Detailed Description
This function serves as the final cleanup phase in the Turkish stemming process, orchestrating three critical post-processing operations:

1. **Reserved Word Check**: First calls  to check if the word should not be stemmed (e.g., "ad", "soyad"). If a reserved word is detected, stemming is terminated and the word is returned unchanged.

2. **Vowel Harmony Restoration**: Calls  to append appropriate vowels to stems ending with 'd' or 'g' consonants, maintaining Turkish vowel harmony rules.

3. **Consonant Processing**: Calls  to apply final consonant transformations, particularly devoicing operations (b→p, c→ç, d→t, ğ→k).

The function processes the string from right to left (backward processing) by setting the left boundary to current position and cursor to the end of the string.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the string being processed and cursor positions
## Dependencies
- Functions called/Symbols referenced:
  - [r_is_reserved_word](r_is_reserved_word.md) (checks for protected words that shouldn't be stemmed)
  - [r_append_U_to_stems_ending_with_d_or_g](r_append_U_to_stems_ending_with_d_or_g.md) (applies vowel harmony rules)
  - [r_post_process_last_consonants](r_post_process_last_consonants.md) (applies final consonant transformations)
- Called from:
  - [turkish_UTF_8_stem](../t/turkish_UTF_8_stem.md) (at src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:2087)

## Notes and Other Information
- This is a static function within the Turkish stemmer implementation
- Returns 0 if a reserved word is detected (no further processing), 1 on successful completion, or negative value on error
- Critical final phase ensuring Turkish morphological and phonological rules are properly applied
- Part of the main stemming pipeline in Turkish word processing
- Generated automatically by Snowball 2.2.0 stemmer generator
- Uses backward processing pattern typical of Snowball stemmers

## Simplified Source

```c
static int r_postlude(struct SN_env * z) {
    int among_var;

    // Process characters to convert uppercase back to lowercase
    while(1) {
        int saved_pos = z->c;
        z->bra = z->c;

        // Look for 'I' (73) or 'Y' (89) characters
        if (z->c >= z->l || (z->p[z->c] != 73 && z->p[z->c] != 89)) {
            among_var = 3;  // No match, advance character
        } else {
            among_var = find_among(z, a_1, 3);  // Find pattern match
        }

        if (!among_var) {
            z->c = saved_pos;
            break;
        }

        z->ket = z->c;

        // Apply transformations based on pattern
        switch (among_var) {
            case 1:
                slice_from_s(z, 1, s_8);  // Convert first pattern (likely 'I' → 'i')
                break;
            case 2:
                slice_from_s(z, 1, s_9);  // Convert second pattern (likely 'Y' → 'y')
                break;
            case 3:
                z->c++;  // Skip character and continue
                break;
        }
    }

    return 1;  // Success
}
```

This function performs post-processing cleanup by:
1. **Character scanning**: Loops through the string looking for uppercase 'I' (ASCII 73) and 'Y' (ASCII 89)
2. **Pattern matching**: Uses pattern table a_1 to identify specific character sequences
3. **Case restoration**: Converts uppercase characters back to their appropriate lowercase forms
4. **Final cleanup**: Ensures text is in proper final form after stemming operations

This is the counterpart to r_prelude, restoring characters that were temporarily modified during stemming.