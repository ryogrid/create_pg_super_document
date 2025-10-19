# r_un_double

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_french.c:1119-1136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_french.c#L1119-L1136)

## Overview
The r_un_double function removes doubled consonants from word endings in the French Snowball stemming algorithm, specifically targeting common doubled consonant patterns at the end of French words.

## Definition

```c
}

static int r_un_double(struct SN_env * z)
```
## Detailed Description
The r_un_double function implements a critical step in French morphological analysis by removing doubled consonants that appear at the end of words. The function operates by first testing for specific doubled consonant patterns using a lookup table (a_8), then removing one instance of the doubled consonant if found. This operation is essential for proper French word stemming as many French words naturally contain doubled consonants that need to be normalized during the stemming process.

The function follows the standard Snowball stemmer pattern: it sets up a test position, searches for matching patterns using find_among_b with array a_8 (which contains 5 doubled consonant patterns), and if a match is found, it removes one character using slice_del. The function uses the standard Snowball boundary markers (bra/ket) to define the text region to be modified.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the word being processed, cursor positions, and stemming boundaries
## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Searches for patterns in the a_8 lookup table
  - [slice_del](../s/slice_del.md): Removes the selected text slice from the word
  - a_8: Lookup table containing 5 doubled consonant patterns
- Called from (representative examples):
  - [french_ISO_8859_1_stem](../f/french_ISO_8859_1_stem.md): Main French stemming function for ISO-8859-1 encoding
  - [french_UTF_8_stem](../f/french_UTF_8_stem.md): Main French stemming function for UTF-8 encoding

## Notes and Other Information
This function is part of the comprehensive French stemming algorithm and works in conjunction with other morphological processing functions like r_un_accent, r_standard_suffix, and r_residual_suffix. The doubled consonant removal is typically performed as part of the final cleanup phase of the stemming process. The function returns 1 on success (when a doubled consonant is found and removed) or 0 when no action is taken.

## Simplified Source

```c
static int r_un_double(struct SN_env * z) {
    // Test for doubled consonant patterns at word end
    int test_pos = z->l - z->c;

    // Check for valid character and pattern match (5 patterns in a_8)
    if (z->c - 2 <= z->lb || z->p[z->c - 1] >> 5 != 3 ||
        !((1069056 >> (z->p[z->c - 1] & 0x1f)) & 1)) {
        return 0;
    }

    if (!find_among_b(z, a_8, 5)) {
        return 0;
    }

    // Restore position after test
    z->c = z->l - test_pos;

    // Mark and remove one instance of doubled consonant
    z->ket = z->c;
    if (z->c <= z->lb) return 0;
    z->c--;
    z->bra = z->c;

    slice_del(z);
    return 1;
}
```