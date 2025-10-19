# r_Step_2

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c:636-728](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c#L636-L728)

## Overview
The r_Step_2 function implements Step 2 of the English Porter stemming algorithm, handling the removal and transformation of various adjectival and noun suffixes within the R1 region.

## Definition
```c
static int r_Step_2(struct SN_env * z)
```

## Detailed Description
This function performs Step 2 of the English stemming algorithm, which processes a comprehensive set of suffixes that form adjectives and nouns. The function operates exclusively within the R1 region to ensure stemming only occurs on substantial word stems.

The function handles 24 different suffix patterns through 15 transformation cases:

**Major suffix transformations**:
- **Case 1**:  →  
- **Cases 2-3**:  → 
- **Case 4**:  → 
- **Cases 5-7**:  → 
- **Cases 8-12**:  → 
- **Case 13**:  (preceded by 'l') → 
- **Case 14**:  → 
- **Case 15**:  (preceded by valid LI characters) → delete

The function uses bit manipulation (815616 mask) for efficient character classification and validates LI-ending suffixes against a specific character set.

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
  - [slice_from_s](../s/slice_from_s.md) (replaces marked substring with specified string)
  - [slice_del](../s/slice_del.md) (deletes the marked substring)
  - [in_grouping_b](../i/in_grouping_b.md) (checks if character is in specified group)
  - a_5 (array of 24 suffixes: anci, enci, ogi, li, bli, abli, alli, fulli, lessli, ousli, entli, aliti, biliti, iviti, tional, ational, alism, ation, ization, izer, ator, iveness, fulness, ousness)
  - s_9 through s_22 (replacement strings: "tion", "ence", "ance", "able", "ent", "ize", "ate", "al", "ful", "ous", "ive", "ble", "og", "less")
  - g_valid_LI (character group for LI suffix validation)
- Called from (representative examples):
  - [english_ISO_8859_1_stem](../e/english_ISO_8859_1_stem.md)
  - [porter_ISO_8859_1_stem](../p/porter_ISO_8859_1_stem.md)  
  - [english_UTF_8_stem](../e/english_UTF_8_stem.md)
  - [porter_UTF_8_stem](../p/porter_UTF_8_stem.md)
  - [serbian_UTF_8_stem](../s/serbian_UTF_8_stem.md)

## Notes and Other Information
- Returns 1 on successful completion, 0 if no changes were made, or negative values on error
- All transformations require the suffix to be in the R1 region, preventing over-aggressive stemming
- Case 13 has additional constraint requiring 'l' before 'ogi' suffix
- Case 15 validates LI-ending against specific character set (c,d,e,g,h,k,m,n,r,t) to avoid incorrect deletions
- Critical component for handling complex English derivational morphology in PostgreSQL's full-text search capabilities

## Simplified Source

```c
static int r_Step_2(struct SN_env * z) {
    // Mark end position for suffix
    z->ket = z->c;

    // Quick character check for efficiency
    if (z->c - 1 <= z->lb || z->p[z->c - 1] >> 5 != 3 ||
        !((815616 >> (z->p[z->c - 1] & 0x1f)) & 1)) {
        return 0;
    }

    // Find matching suffix from predefined list
    int among_var = find_among_b(z, a_5, 24);
    if (!among_var) return 0;

    z->bra = z->c;

    // Ensure we're in R1 region
    if (r_R1(z) <= 0) return 0;

    // Apply appropriate transformation based on suffix
    switch (among_var) {
        case 1: return slice_from_s(z, 4, s_9);   // -> "tion"
        case 2: return slice_from_s(z, 4, s_10);  // -> "ence"
        case 3: return slice_from_s(z, 4, s_11);  // -> "ance"
        case 4: return slice_from_s(z, 4, s_12);  // -> "able"
        case 5: return slice_from_s(z, 3, s_13);  // -> "ent"
        case 6: return slice_from_s(z, 3, s_14);  // -> "ize"
        case 7: return slice_from_s(z, 3, s_15);  // -> "ate"
        case 8: return slice_from_s(z, 2, s_16);  // -> "al"
        case 9: return slice_from_s(z, 3, s_17);  // -> "ful"
        case 10: return slice_from_s(z, 3, s_18); // -> "ous"
        case 11: return slice_from_s(z, 3, s_19); // -> "ive"
        case 12: return slice_from_s(z, 3, s_20); // -> "ble"
        case 13: // Special case: 'ogi' preceded by 'l' -> "og"
            if (z->c <= z->lb || z->p[z->c - 1] != 'l') return 0;
            z->c--;
            return slice_from_s(z, 2, s_21);
        case 14: return slice_from_s(z, 4, s_22); // -> "less"
        case 15: // 'li' suffixes - delete if preceded by valid characters
            if (in_grouping_b(z, g_valid_LI, 99, 116, 0)) return 0;
            return slice_del(z);
    }
    return 1;
}
```