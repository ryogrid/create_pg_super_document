# r_tidy

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_finnish.c:573-657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_finnish.c#L573-L657)

## Overview
The r_tidy function performs final cleanup operations during Finnish text stemming, removing redundant characters and normalizing vowel-consonant patterns to produce the final stemmed form.

## Definition
static int r_tidy(struct SN_env * z)

## Detailed Description
The r_tidy function is the final stage in the Finnish stemming algorithm that performs several cleanup operations to normalize the stemmed word. It operates within the R1 region (established by r_mark_regions) and performs the following operations:

1. **Long vowel normalization**: Uses the r_LONG function to detect long vowel patterns and removes redundant vowel characters
2. **Vowel-consonant pattern cleanup**: Removes specific vowel characters (AEI group: a, e, i, ä) that precede consonants in certain patterns
3. **Suffix cleanup**: Removes specific endings like 'oj' and 'uj' (masculine/neuter endings) and 'jo' patterns
4. **Final normalization**: Performs a final check to ensure vowel-consonant patterns are normalized, removing duplicate consonant patterns

The function uses backward processing (from end to beginning of the word) and employs various character group tests to identify valid patterns for removal.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the word being processed and stemming state information

## Dependencies
- Functions called/Symbols referenced:
  - [r_LONG](r_LONG.md)
  - [in_grouping_b](../i/in_grouping_b.md)
  - [slice_del](../s/slice_del.md)
  - [slice_to](../s/slice_to.md)
  - [eq_v_b](../e/eq_v_b.md)
- Called from (representative examples):
  - [finnish_ISO_8859_1_stem](../f/finnish_ISO_8859_1_stem.md)
  - [finnish_UTF_8_stem](../f/finnish_UTF_8_stem.md)

## Notes and Other Information
- This function is always called as the final step in the Finnish stemming process after all morphological endings have been removed
- Returns 1 on successful completion, 0 if no changes were made, or negative values on error
- The function operates only within the R1 region boundary to avoid over-stemming
- Uses character groups g_AEI, g_C, and g_V1 for pattern matching
- The cleanup operations are designed specifically for Finnish morphological patterns and may produce unexpected results on non-Finnish text

## Simplified Source

```c
static int r_tidy(struct SN_env * z) {
    // Work within R1 region for all cleanup operations
    {
        int mlimit1;
        if (z->c < z->I[1]) return 0;
        mlimit1 = z->lb; z->lb = z->I[1];

        // 1. Long vowel normalization - remove redundant characters after LONG patterns
        {
            int m_test = z->l - z->c;
            {
                int m_inner = z->l - z->c;
                if (r_LONG(z)) {
                    z->c = z->l - m_inner;
                    z->ket = z->c;
                    if (z->c > z->lb) {
                        z->c--;
                        z->bra = z->c;
                        if (slice_del(z) < 0) return -1;
                    }
                }
            }
            z->c = z->l - m_test;
        }

        // 2. Remove AEI vowels before consonants
        {
            int m_test = z->l - z->c;
            z->ket = z->c;
            if (!in_grouping_b(z, g_AEI, 97, 228, 0)) {  // If current char is AEI
                z->bra = z->c;
                if (!in_grouping_b(z, g_C, 98, 122, 0)) {  // And next is consonant
                    if (slice_del(z) < 0) return -1;
                }
            }
            z->c = z->l - m_test;
        }

        // 3. Remove 'oj' and 'uj' endings
        {
            int m_test = z->l - z->c;
            z->ket = z->c;
            if (z->c > z->lb && z->p[z->c - 1] == 'j') {
                z->c--;
                z->bra = z->c;
                // Check for 'o' or 'u' before 'j'
                {
                    int m_inner = z->l - z->c;
                    if (z->c > z->lb && z->p[z->c - 1] == 'o') {
                        z->c--;
                    } else {
                        z->c = z->l - m_inner;
                        if (z->c > z->lb && z->p[z->c - 1] == 'u') {
                            z->c--;
                        } else {
                            goto skip_oj_uj;
                        }
                    }
                }
                if (slice_del(z) < 0) return -1;
            }
        skip_oj_uj:
            z->c = z->l - m_test;
        }

        // 4. Remove 'jo' endings
        {
            int m_test = z->l - z->c;
            z->ket = z->c;
            if (z->c > z->lb && z->p[z->c - 1] == 'o') {
                z->c--;
                z->bra = z->c;
                if (z->c > z->lb && z->p[z->c - 1] == 'j') {
                    z->c--;
                    if (slice_del(z) < 0) return -1;
                }
            }
            z->c = z->l - m_test;
        }

        z->lb = mlimit1;
    }

    // Final normalization: remove duplicate consonants after vowels
    if (in_grouping_b(z, g_V1, 97, 246, 1) < 0) return 0;  // Find vowel
    z->ket = z->c;
    if (in_grouping_b(z, g_C, 98, 122, 0)) return 0;  // Must be followed by consonant
    z->bra = z->c;

    // Extract consonant and check if it's duplicated
    z->S[0] = slice_to(z, z->S[0]);
    if (z->S[0] == 0) return -1;
    if (!eq_v_b(z, z->S[0])) return 0;  // Check if same consonant appears again

    // Remove the duplicate consonant
    if (slice_del(z) < 0) return -1;
    return 1;
}
```