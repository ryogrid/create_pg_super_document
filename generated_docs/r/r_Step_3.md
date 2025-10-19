# r_Step_3

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c:729-776](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c#L729-L776)

## Overview
The r_Step_3 function implements Step 3 of the English Porter stemming algorithm, handling the removal and transformation of specific adjectival and nominal suffixes within the R1 and R2 regions.

## Definition
```c
static int r_Step_3(struct SN_env * z)
```

## Detailed Description
This function performs Step 3 of the English stemming algorithm, processing 9 different suffixes that form adjectives and nouns. The function requires all transformations to occur within the R1 region, with some cases requiring additional R2 region validation.

The function handles specific suffix transformations through 6 cases:

**Suffix transformations**:
- **Case 1**:  →  (requires R1)
- **Case 2**:  →  (requires R1)  
- **Case 3**:  →  (requires R1)
- **Case 4**:  →  (requires R1)
- **Case 5**:  → delete (requires R1)
- **Case 6**:  → delete (requires R2)

The function uses bit manipulation (528928 mask) for efficient character classification and applies stricter regional constraints than earlier steps, with Case 6 requiring the more restrictive R2 region.

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
  - [r_R1](r_R1.md) (tests if position is in R1 region)
  - [r_R2](r_R2.md) (tests if position is in R2 region)
  - [slice_from_s](../s/slice_from_s.md) (replaces marked substring with specified string)
  - [slice_del](../s/slice_del.md) (deletes the marked substring)
  - a_6 (array of 9 suffixes: icate, ative, alize, iciti, ical, tional, ational, ful, ness)
  - s_23, s_24, s_25, s_26 (replacement strings: "tion", "ate", "al", "ic")
- Called from (representative examples):
  - [english_ISO_8859_1_stem](../e/english_ISO_8859_1_stem.md)
  - [porter_ISO_8859_1_stem](../p/porter_ISO_8859_1_stem.md)
  - [english_UTF_8_stem](../e/english_UTF_8_stem.md)
  - [porter_UTF_8_stem](../p/porter_UTF_8_stem.md)
  - [serbian_UTF_8_stem](../s/serbian_UTF_8_stem.md)

## Notes and Other Information
- Returns 1 on successful completion, 0 if no changes were made, or negative values on error
- All transformations require the suffix to be in the R1 region minimum
- Case 6 (ative) has stricter requirement, needing R2 region for deletion
- Handles complex derivational suffixes that form adjectives and abstract nouns
- More conservative than Steps 1-2, applying transformations only to substantial word stems
- Final major suffix processing step before Steps 4-5 handle shorter, more common suffixes
- Essential for proper handling of Latinate vocabulary in English stemming within PostgreSQL's full-text search

## Simplified Source

```c
static int r_Step_3(struct SN_env * z) {
    // Mark end position for suffix
    z->ket = z->c;

    // Quick character check for efficiency
    if (z->c - 2 <= z->lb || z->p[z->c - 1] >> 5 != 3 ||
        !((528928 >> (z->p[z->c - 1] & 0x1f)) & 1)) {
        return 0;
    }

    // Find matching suffix from predefined list
    int among_var = find_among_b(z, a_6, 9);
    if (!among_var) return 0;

    z->bra = z->c;

    // Ensure we're in R1 region
    if (r_R1(z) <= 0) return 0;

    // Apply appropriate transformation based on suffix
    switch (among_var) {
        case 1: return slice_from_s(z, 4, s_23); // icate -> "tion"
        case 2: return slice_from_s(z, 3, s_24); // ative -> "ate"
        case 3: return slice_from_s(z, 2, s_25); // alize -> "al"
        case 4: return slice_from_s(z, 2, s_26); // iciti -> "ic"
        case 5: return slice_del(z);             // ical/ful/ness -> delete
        case 6: // ative -> delete (requires R2 region)
            if (r_R2(z) <= 0) return 0;
            return slice_del(z);
    }
    return 1;
}
```