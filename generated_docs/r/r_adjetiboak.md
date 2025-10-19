# r_adjetiboak

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_basque.c:1117-1141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_basque.c#L1117-L1141)

## Overview
A Basque-specific stemming function that handles adjective suffix processing ("adjetiboak" means "adjectives" in Basque), implementing rule-based suffix removal and transformation for adjective forms during text normalization.

## Definition
static int r_adjetiboak(struct SN_env * z)

## Detailed Description
The r_adjetiboak function processes Basque adjective endings during stemming by matching against a predefined set of 19 adjective suffix patterns (a_2 array). It operates by positioning cursors at word boundaries, identifying matching suffix patterns, and applying appropriate transformations based on morphological rules. The function uses a simple switch statement with 2 cases: one for suffix deletion within the RV region and another for suffix replacement with a specific string (s_10). This is part of the Snowball stemming algorithm implementation for Basque language support in PostgreSQL's full-text search functionality.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemming environment with word data, cursors, and boundaries

## Dependencies
- Functions called/Symbols referenced:
  - [r_RV](r_RV.md) (region boundary test)
  - [find_among_b](../f/find_among_b.md) (suffix matching)
  - [slice_del](../s/slice_del.md) (suffix deletion)
  - [slice_from_s](../s/slice_from_s.md) (suffix replacement)
- Called from (representative examples):
  - [basque_ISO_8859_1_stem](../b/basque_ISO_8859_1_stem.md)
  - [basque_UTF_8_stem](../b/basque_UTF_8_stem.md)

## Notes and Other Information
This function is part of the Basque stemming algorithm and specifically handles adjective morphology. It processes 19 different adjective suffixes with simpler logic compared to noun processing (r_izenak). The function returns 1 on successful processing and 0 if no matching suffix is found. The relatively small number of patterns (19 vs 295 for nouns) reflects the simpler morphological structure of Basque adjectives compared to nouns.

## Simplified Source

```c
static int r_adjetiboak(struct SN_env * z) {
    int suffix_type;

    // Set suffix end boundary and validate character class
    z->ket = z->c;
    if (z->c - 1 <= z->lb || !valid_character_class(z->p[z->c - 1]))
        return 0;

    // Find matching adjective suffix from pattern array (19 patterns)
    suffix_type = find_among_b(z, a_2, 19);
    if (!suffix_type) return 0;

    // Set suffix start boundary
    z->bra = z->c;

    // Process based on suffix type
    switch (suffix_type) {
        case 1:  // Standard adjective suffix - requires RV region
            if (!r_RV(z)) return 0;
            slice_del(z);  // Delete suffix
            break;

        case 2:  // Replace with canonical form
            slice_from_s(z, 1, s_10);
            break;
    }

    return 1;
}
```