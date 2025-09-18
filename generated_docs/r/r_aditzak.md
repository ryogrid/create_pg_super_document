# r_aditzak

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_1_basque.c: 1001 - 1043

## Overview
This function handles the removal of Basque verb suffixes ("aditzak" means "verbs" in Basque) as part of the Basque language stemming algorithm in PostgreSQL's Snowball stemmer.

## Definition
```c
static int r_aditzak(struct SN_env * z)
```

## Detailed Description
The `r_aditzak` function is a specialized component of the Basque stemmer that identifies and processes verb suffixes. It implements a suffix-matching algorithm using the Snowball framework's pattern-matching capabilities to:

1. **Suffix Identification**: Uses `find_among_b` to search backward from the current position for known verb suffixes from array `a_0` (109 patterns)
2. **Boundary Validation**: Ensures the cursor is positioned within valid morphological boundaries (RV or R2 regions)
3. **Suffix Processing**: Based on the matched pattern type, either:
   - Deletes the suffix (cases 1-2) with appropriate region checks
   - Replaces the suffix with canonical forms (cases 3-5) using predefined strings

The function performs sophisticated morphological analysis by checking bit patterns and using region boundaries to ensure safe suffix removal without over-stemming. This is essential for Basque, which has complex verb conjugation patterns.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing:
  - `z->c`: Current cursor position in the word
  - `z->ket`: End position for suffix matching
  - `z->bra`: Beginning position for suffix matching  
  - `z->p`: Pointer to the word being processed
  - `z->lb`: Lower boundary for the word

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward suffix pattern matching)
  - [r_RV](r_RV.md) (RV region boundary check)
  - [r_R2](r_R2.md) (R2 region boundary check)  
  - [slice_del](../s/slice_del.md) (suffix deletion)
  - [slice_from_s](../s/slice_from_s.md) (suffix replacement)
- Called from (representative examples):
  - [basque_ISO_8859_1_stem](../b/basque_ISO_8859_1_stem.md) (main Basque stemming function)
  - [basque_UTF_8_stem](../b/basque_UTF_8_stem.md) (UTF-8 variant)

## Notes and Other Information
- Returns 1 on successful processing, 0 if no matching suffix found
- Handles 109 different Basque verb suffix patterns in array `a_0`
- Uses bit manipulation for efficient character class checking
- Critical for proper handling of Basque's rich verbal morphology
- Part of the comprehensive Basque language support in PostgreSQL's full-text search
- The "aditzak" terminology reflects the function's specific focus on Basque verb forms