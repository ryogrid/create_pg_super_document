# r_Suffix_Verb_Step1

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_arabic.c:1303-1331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_arabic.c#L1303-L1331)

## Overview
This function performs Step 1 of verb suffix removal in the Arabic stemming algorithm, handling multiple categories of Arabic verb suffixes with different minimum length requirements.

## Definition
```c
static int r_Suffix_Verb_Step1(struct SN_env * z)
```

## Detailed Description
The function implements the first step of verb suffix removal in the Arabic stemming process. It operates by:
1. Setting the current cursor position as the end boundary (ket)
2. Using pattern matching with predefined suffix array a_17 (containing 12 different verb suffixes)
3. Applying different minimum length requirements based on the matched suffix category:
   - Category 1: Requires minimum 4 UTF-8 characters after removal
   - Category 2: Requires minimum 5 UTF-8 characters after removal  
   - Category 3: Requires minimum 6 UTF-8 characters after removal
4. Deleting the identified suffix if length requirements are met

This stepped approach ensures that shorter verb forms are not over-stemmed, preserving the linguistic integrity of Arabic words.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment with:
  - `c`: Current cursor position in the string
  - `ket`: End boundary marker for suffix identification
  - `bra`: Start boundary marker for suffix identification
  - `p`: Pointer to the string being processed
- `among_var`: Local variable storing the category of matched suffix (1, 2, or 3)

## Dependencies
- Functions called/Symbols referenced:
  - [len_utf8](../l/len_utf8.md): Used to check minimum word length requirements for each suffix category
  - [find_among_b](../f/find_among_b.md): Used for backward suffix pattern matching with array a_17
  - [slice_del](../s/slice_del.md): Used to delete the identified suffix from the string
- Called from (representative examples):
  - [arabic_UTF_8_stem](../a/arabic_UTF_8_stem.md): Main Arabic stemming function that orchestrates the stemming process

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit
- Uses array a_17 containing 12 different Arabic verb suffixes for pattern matching
- Implements a tiered approach with three different minimum length thresholds (4, 5, 6 characters)
- The varying length requirements reflect the morphological complexity of different Arabic verb suffixes
- Part of the comprehensive Arabic verb stemming process that includes multiple sequential steps
- Returns 1 on successful suffix removal and 0 when no applicable suffix is found

## Simplified Source

```c
static int r_Suffix_Verb_Step1(struct SN_env * z) {
    int suffix_category;

    // Mark current position as end boundary for suffix matching
    z->ket = z->c;

    // Find matching verb suffix from predefined array (12 suffixes)
    suffix_category = find_among_b(z, a_17, 12);
    if (!suffix_category) return 0;  // No suffix found

    // Mark start boundary for deletion
    z->bra = z->c;

    // Apply different minimum length requirements based on suffix type
    switch (suffix_category) {
        case 1:  // Category 1: minimum 4 UTF-8 characters
            if (len_utf8(z->p) >= 4) {
                slice_del(z);  // Delete the suffix
            } else {
                return 0;  // Word too short, don't remove suffix
            }
            break;

        case 2:  // Category 2: minimum 5 UTF-8 characters
            if (len_utf8(z->p) >= 5) {
                slice_del(z);
            } else {
                return 0;
            }
            break;

        case 3:  // Category 3: minimum 6 UTF-8 characters
            if (len_utf8(z->p) >= 6) {
                slice_del(z);
            } else {
                return 0;
            }
            break;
    }

    return 1;  // Successfully processed suffix
}
```