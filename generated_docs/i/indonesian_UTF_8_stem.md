# indonesian_UTF_8_stem

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:313-403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c#L313-L403)

## Overview
The main stemming function for Indonesian text using UTF-8 encoding that implements the Snowball Indonesian stemming algorithm to reduce words to their root forms.

## Definition

```c
}

extern int indonesian_UTF_8_stem(struct SN_env * z)
```
## Detailed Description
This function implements the complete Indonesian stemming algorithm as part of the Snowball stemmer library. It processes Indonesian words by:

1. **Vowel counting**: First counts vowels in the word to ensure sufficient length (must have more than 2 vowels)
2. **Particle removal**: Removes particle suffixes from the end of the word
3. **Possessive pronoun removal**: Removes possessive pronoun suffixes  
4. **Prefix removal strategy**: Attempts two different approaches:
   - First tries to remove first-order prefixes, then optionally removes suffixes and second-order prefixes
   - If first-order prefix removal fails, tries second-order prefix removal followed by optional suffix removal

The algorithm uses careful backtracking and position management to ensure proper morphological analysis while maintaining the word's integrity.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure (SN_env) containing the word being processed and algorithm state
  - : Used for algorithm state tracking
  - : Stores the vowel count of the word
  - : Current cursor position in the word
  - : Length of the word
  - : Left boundary for processing

## Dependencies
- Functions called/Symbols referenced:
  - : Moves cursor past vowel characters for vowel counting
  - : Removes particle suffixes ('lah', 'kah', 'tah', 'pun')
  - : Removes possessive pronoun suffixes ('ku', 'mu', 'nya')
  - : Removes first-order prefixes ('di', 'ke', 'me', etc.)
  - : Removes suffixes ('i', 'an', 'kan')
  - : Removes second-order prefixes ('be', 'ber', 'pe', 'per')
- Called from: This is the main entry point for Indonesian stemming and is not called by other functions in the codebase

## Notes and Other Information
- The function returns 1 on successful stemming, 0 if the word is too short to stem (≤2 vowels)
- Uses a multi-stage approach typical of Snowball stemmers with careful position management
- Implements the official Indonesian stemming rules as defined in the Snowball algorithm
- The vowel count check (I[1] > 2) is performed multiple times throughout to prevent over-stemming of short words
- Position saving/restoring (c5, c7, c9, c10, etc.) allows for backtracking when certain removal operations fail

## Simplified Source

```c
extern int indonesian_UTF_8_stem(struct SN_env * z) {
    // Step 1: Count vowels to ensure word is long enough
    z->I[1] = 0; // vowel counter
    int c1 = z->c;
    while(1) {
        int c2 = z->c;
        // Move past vowel characters
        int ret = out_grouping_U(z, g_vowel, 97, 117, 1);
        if (ret < 0) break;
        z->c += ret;
        z->I[1] += 1;
    }
    z->c = c1;

    // Must have more than 2 vowels to proceed
    if (!(z->I[1] > 2)) return 0;

    // Step 2: Process from end of word
    z->lb = z->c; z->c = z->l;

    // Remove particles (lah, kah, tah, pun)
    r_remove_particle(z);
    if (!(z->I[1] > 2)) return 0;

    // Remove possessive pronouns (ku, mu, nya)
    r_remove_possessive_pronoun(z);
    z->c = z->lb;
    if (!(z->I[1] > 2)) return 0;

    // Step 3: Try prefix removal strategies
    int c5 = z->c;

    // Strategy A: Try first-order prefix removal
    if (r_remove_first_order_prefix(z)) {
        // If successful, optionally remove suffix and second-order prefix
        if (z->I[1] > 2) {
            z->lb = z->c; z->c = z->l;
            r_remove_suffix(z);
            z->c = z->lb;
        }
        if (z->I[1] > 2) {
            r_remove_second_order_prefix(z);
        }
    } else {
        // Strategy B: Try second-order prefix, then optional suffix
        z->c = c5;
        r_remove_second_order_prefix(z);
        if (z->I[1] > 2) {
            z->lb = z->c; z->c = z->l;
            r_remove_suffix(z);
            z->c = z->lb;
        }
    }

    return 1; // Success
}
```