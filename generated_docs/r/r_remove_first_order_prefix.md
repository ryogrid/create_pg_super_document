# r_remove_first_order_prefix

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:194-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c#L194-L273)

## Overview
Removes first-order prefixes from Indonesian words including 'di', 'ke', 'me', 'mem', 'men', 'meng', 'meny', 'pem', 'pen', 'peng', 'peny', and 'ter' with appropriate morphological transformations.

## Definition

```c
}

static int r_remove_first_order_prefix(struct SN_env * z)
```
## Detailed Description
This function implements the removal of Indonesian first-order prefixes as part of the Snowball stemming algorithm. It searches for prefixes defined in the  array and performs different transformations based on the matched prefix type. The function handles complex morphological rules including consonant restoration (adding 's' or 'p' characters), vowel-based conditional transformations, and proper morphological type tracking through  and word length adjustment through . Each case represents different morphological patterns: simple deletion, consonant restoration, or conditional vowel-based transformations.

## Parameters / Member Variables
- `*z`: Pointer to SN_env structure containing the stemmer environment with the word being processed
## Dependencies
- Functions called/Symbols referenced:
  - [find_among](../f/find_among.md) (Snowball framework function for forward string matching)
  - [slice_del](../s/slice_del.md) (Snowball framework function for string deletion) 
  - [slice_from_s](../s/slice_from_s.md) (Snowball framework function for string replacement)
  - [in_grouping](../i/in_grouping.md) (Snowball framework function for character group testing)
  - a_3 (array defining 12 first-order prefixes with their patterns)
  - s_1, s_2, s_3, s_4 (string constants for consonant restoration)
  - g_vowel (character grouping for vowel testing)
- Called from (representative examples):
  - [indonesian_ISO_8859_1_stem](../i/indonesian_ISO_8859_1_stem.md) (src/backend/snowball/libstemmer/stem_ISO_8859_1_indonesian.c:352)
  - [indonesian_UTF_8_stem](../i/indonesian_UTF_8_stem.md) (src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:352)

## Notes and Other Information
- Part of PostgreSQL's full-text search Snowball stemming implementation for Indonesian language
- Returns 1 on successful prefix removal, 0 if no applicable prefix found, or negative value on error
- Uses morphological type codes: I[0]=1 for certain prefixes, I[0]=3 for others
- Includes complex vowel-based conditional logic for 'mem' and 'pem' prefixes (cases 5 and 6)
- Pre-checks for ending characters (105='i', 101='e') for performance optimization
- Generated automatically by Snowball compiler from Indonesian stemming rules

## Simplified Source

```c
static int r_remove_first_order_prefix(struct SN_env * z) {
    // Set start marker
    z->bra = z->c;

    // Quick check: prefix must end with 'i' or 'e'
    if (z->c + 1 >= z->l || (z->p[z->c + 1] != 105 && z->p[z->c + 1] != 101))
        return 0;

    // Find matching first-order prefix pattern
    int among_var = find_among(z, a_3, 12);
    if (!(among_var)) return 0;

    z->ket = z->c;

    switch (among_var) {
        case 1: // Simple prefixes (di, ke, ter)
            slice_del(z);
            z->I[0] = 1;
            break;

        case 2: // Complex prefixes (men, pen)
            slice_del(z);
            z->I[0] = 3;
            break;

        case 3: // Restore 's' (meny -> s)
            z->I[0] = 1;
            slice_from_s(z, 1, s_1); // s_1 = "s"
            break;

        case 4: // Restore 'p' (peny -> p)
            z->I[0] = 3;
            slice_from_s(z, 1, s_2); // s_2 = "p"
            break;

        case 5: // mem prefix - conditional restoration
            z->I[0] = 1;
            // If next char is vowel, add 'p'; otherwise delete
            if (in_grouping(z, g_vowel, 97, 117, 0)) {
                slice_del(z);
            } else {
                slice_from_s(z, 1, s_3); // s_3 = "p"
            }
            break;

        case 6: // pem prefix - conditional restoration
            z->I[0] = 3;
            // If next char is vowel, add 'p'; otherwise delete
            if (in_grouping(z, g_vowel, 97, 117, 0)) {
                slice_del(z);
            } else {
                slice_from_s(z, 1, s_4); // s_4 = "p"
            }
            break;
    }

    z->I[1] -= 1; // Track removal count
    return 1;
}
```