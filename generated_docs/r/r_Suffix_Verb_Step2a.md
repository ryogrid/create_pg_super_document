# r_Suffix_Verb_Step2a

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_arabic.c:1332-1366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_arabic.c#L1332-L1366)

## Overview
This function performs Step 2a of verb suffix removal in the Arabic stemming algorithm, handling a specific set of Arabic verb suffixes with four different length requirement categories.

## Definition
```c
static int r_Suffix_Verb_Step2a(struct SN_env * z)
```

## Detailed Description
The function implements Step 2a of the verb suffix removal process in Arabic stemming. It operates by:
1. Setting the current cursor position as the end boundary (ket)
2. Using pattern matching with predefined suffix array a_18 (containing 11 different verb suffixes)
3. Applying different minimum length requirements based on the matched suffix category:
   - Category 1: Requires minimum 4 UTF-8 characters after removal
   - Category 2: Requires minimum 5 UTF-8 characters after removal
   - Category 3: Requires more than 5 UTF-8 characters after removal (> 5, not >= 5)
   - Category 4: Requires minimum 6 UTF-8 characters after removal
4. Deleting the identified suffix if length requirements are met

This step is part of a multi-phase verb stemming process and handles a different set of suffixes than Step 1.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment with:
  - `c`: Current cursor position in the string
  - `ket`: End boundary marker for suffix identification
  - `bra`: Start boundary marker for suffix identification
  - `p`: Pointer to the string being processed
- `among_var`: Local variable storing the category of matched suffix (1, 2, 3, or 4)

## Dependencies
- Functions called/Symbols referenced:
  - [len_utf8](../l/len_utf8.md): Used to check minimum word length requirements for each suffix category
  - [find_among_b](../f/find_among_b.md): Used for backward suffix pattern matching with array a_18
  - [slice_del](../s/slice_del.md): Used to delete the identified suffix from the string
- Called from (representative examples):
  - [arabic_UTF_8_stem](../a/arabic_UTF_8_stem.md): Main Arabic stemming function that orchestrates the stemming process (called twice at lines 1450 and 1480)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit
- Uses array a_18 containing 11 different Arabic verb suffixes for pattern matching
- Implements a four-tiered approach with different minimum length thresholds (4, 5, >5, 6 characters)
- Note the distinction in Category 3 which uses '>' instead of '>=' for the length check
- Called multiple times in the main stemming function, indicating it handles suffixes that may appear in different contexts
- Part of the sequential verb stemming process alongside other Step2 functions (Step2b, Step2c)
- Returns 1 on successful suffix removal and 0 when no applicable suffix is found

## Simplified Source

```c
static int r_Suffix_Verb_Step2a(struct SN_env * z) {
    int suffix_category;

    // Mark current position as end boundary for suffix matching
    z->ket = z->c;

    // Find matching verb suffix from predefined array (11 suffixes)
    suffix_category = find_among_b(z, a_18, 11);
    if (!suffix_category) return 0;  // No suffix found

    // Mark start boundary for deletion
    z->bra = z->c;

    // Apply different minimum length requirements based on suffix type
    switch (suffix_category) {
        case 1:  // Category 1: minimum 4 UTF-8 characters
            if (len_utf8(z->p) >= 4) {
                slice_del(z);  // Delete the suffix
            } else {
                return 0;  // Word too short
            }
            break;

        case 2:  // Category 2: minimum 5 UTF-8 characters
            if (len_utf8(z->p) >= 5) {
                slice_del(z);
            } else {
                return 0;
            }
            break;

        case 3:  // Category 3: MORE than 5 UTF-8 characters (strict)
            if (len_utf8(z->p) > 5) {
                slice_del(z);
            } else {
                return 0;
            }
            break;

        case 4:  // Category 4: minimum 6 UTF-8 characters
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