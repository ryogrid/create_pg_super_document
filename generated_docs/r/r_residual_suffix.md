# r_residual_suffix

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_spanish.c:941-987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_spanish.c#L941-L987)

## Overview
This function handles residual suffixes that remain after other suffix processing steps, performing final cleanup operations with either deletion or replacement.

## Definition
```c
static int r_residual_suffix(struct SN_env * z)
```

## Detailed Description
The `r_residual_suffix` function serves as a final cleanup step in the Snowball stemming algorithm, processing suffixes that may have been missed or created by previous stemming operations. It operates on a smaller, focused set of 22 suffix patterns (stored in array a_4) and applies two distinct processing strategies:

1. **Category 1**: Residual suffixes that require R1 region validation and complete deletion
2. **Category 2**: Residual suffixes that require R1 region validation and replacement with a 2-character string (s_9)

This function typically runs after the main suffix removal operations (standard suffixes, verb suffixes, etc.) have been completed. Its purpose is to catch any remaining morphological elements that need to be addressed for optimal stemming results. The relatively small number of patterns (22) reflects its specialized role as a cleanup function rather than a primary morphological processor.

The function ensures that words don't retain inappropriate suffixes after the main stemming steps, helping to achieve more consistent and accurate stem forms.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the stemming environment, including:
  - `c`: Current cursor position
  - `ket`: End position marker
  - `bra`: Start position marker
  - `p`: Pointer to the string being processed
- `among_var`: Local variable storing the category number (1-2) of the matched residual suffix pattern

## Dependencies
- Functions called/Symbols referenced:
  - [r_R1](r_R1.md) (tests if position is within R1 region)
  - [find_among_b](../f/find_among_b.md) (backward pattern matching function)
  - [slice_del](../s/slice_del.md) (deletes text between bra and ket positions)
  - [slice_from_s](../s/slice_from_s.md) (replaces text between bra and ket with specified string)
- Called from (representative examples):
  - [catalan_ISO_8859_1_stem](../c/catalan_ISO_8859_1_stem.md) (src/backend/snowball/libstemmer/stem_ISO_8859_1_catalan.c:1428)
  - [french_ISO_8859_1_stem](../f/french_ISO_8859_1_stem.md) (src/backend/snowball/libstemmer/stem_ISO_8859_1_french.c:1218)
  - [portuguese_ISO_8859_1_stem](../p/portuguese_ISO_8859_1_stem.md) (src/backend/snowball/libstemmer/stem_ISO_8859_1_portuguese.c:933)
  - [spanish_ISO_8859_1_stem](../s/spanish_ISO_8859_1_stem.md) (src/backend/snowball/libstemmer/stem_ISO_8859_1_spanish.c:1023)

## Notes and Other Information
- This function processes only 22 patterns, significantly fewer than other suffix functions, reflecting its specialized cleanup role
- Both processing categories require R1 region validation, indicating these are still morphologically significant elements
- The function handles both deletion and replacement operations, showing that some residual patterns need transformation rather than simple removal
- It is used across multiple Romance language stemmers (Catalan, French, Portuguese, Spanish) but notably not in Italian or other languages
- Returns 1 on successful processing, 0 if no residual suffix matched, and negative values on error
- The replacement string s_9 is language-specific and typically represents a common morphological ending
- This function typically runs as one of the final steps in the stemming algorithm, after main morphological processing
- The static function scope indicates it's only used within specific stemmer implementation files

## Simplified Source

```c
static int r_residual_suffix(struct SN_env * z) {
    // Set end boundary and search for residual suffix pattern
    z->ket = z->c;
    int residual_category = find_among_b(z, a_4, 22);

    if (!residual_category) {
        return 0;  // No residual suffix pattern found
    }

    z->bra = z->c;  // Set start boundary

    // Apply cleanup operation based on residual suffix category
    switch (residual_category) {
        case 1:
            // Category 1: Complete deletion of residual suffix
            if (r_R1(z)) {
                return slice_del(z);
            }
            break;

        case 2:
            // Category 2: Replace with normalized ending (s_9)
            if (r_R1(z)) {
                return slice_from_s(z, 2, s_9);
            }
            break;
    }

    return 1;  // Pattern found but R1 region requirement not met
}
```