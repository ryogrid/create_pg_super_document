# r_Step_3

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c: 729 - 776

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
  - find_among_b (searches backwards for matching suffixes)
  - r_R1 (tests if position is in R1 region)
  - r_R2 (tests if position is in R2 region)
  - slice_from_s (replaces marked substring with specified string)
  - slice_del (deletes the marked substring)
  - a_6 (array of 9 suffixes: icate, ative, alize, iciti, ical, tional, ational, ful, ness)
  - s_23, s_24, s_25, s_26 (replacement strings: "tion", "ate", "al", "ic")
- Called from (representative examples):
  - english_ISO_8859_1_stem
  - porter_ISO_8859_1_stem
  - english_UTF_8_stem
  - porter_UTF_8_stem
  - serbian_UTF_8_stem

## Notes and Other Information
- Returns 1 on successful completion, 0 if no changes were made, or negative values on error
- All transformations require the suffix to be in the R1 region minimum
- Case 6 (ative) has stricter requirement, needing R2 region for deletion
- Handles complex derivational suffixes that form adjectives and abstract nouns
- More conservative than Steps 1-2, applying transformations only to substantial word stems
- Final major suffix processing step before Steps 4-5 handle shorter, more common suffixes
- Essential for proper handling of Latinate vocabulary in English stemming within PostgreSQL's full-text search