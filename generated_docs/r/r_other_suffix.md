# r_other_suffix

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_danish.c:210-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_danish.c#L210-L254)

## Overview
This function handles secondary suffix removal in Scandinavian stemming, processing additional suffix patterns and performing consonant pair cleanup after the main suffix removal phase.

## Definition

```c
}

static int r_other_suffix(struct SN_env * z)
```
## Detailed Description
The r_other_suffix function implements a two-stage suffix processing algorithm for Scandinavian languages:

**Stage 1**: Handles a specific compound suffix pattern using exact string matching:
- Searches for a 2-character suffix pattern (s_0)  
- If found, looks for an additional 2-character pattern (s_1) before it
- If both patterns match, deletes the compound suffix

**Stage 2**: Processes secondary suffixes using pattern matching:
- Operates within the region boundary (I[1]) established by r_mark_regions
- Uses character-level filtering (bit manipulation with 1572992) for performance
- Matches against a predefined list of 5 secondary suffix patterns (a_2)
- Executes different actions based on the matched pattern:
  - Case 1: Deletes the suffix and calls r_consonant_pair to handle doubled consonants
  - Case 2: Replaces the suffix with a 3-character string (s_2)

The function uses test position mechanism to preserve cursor state during the first stage processing.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the word being processed
  - : Current cursor position in the word
  - : Length of the word  
  - : Region boundary marker (from r_mark_regions)
  - : Left boundary limit
  - : End position of matched substring
  - : Beginning position of matched substring
  - : Pointer to the word being processed

## Dependencies
- Functions called/Symbols referenced:
  - [eq_s_b](../e/eq_s_b.md) (exact backward string matching for s_0 and s_1 patterns)
  - [find_among_b](../f/find_among_b.md) (backward pattern matching against secondary suffix list a_2)
  - [slice_del](../s/slice_del.md) (suffix deletion operation)
  - [slice_from_s](../s/slice_from_s.md) (suffix replacement with s_2 string)
  - [r_consonant_pair](r_consonant_pair.md) (doubled consonant cleanup)
  - s_0, s_1 (2-character string patterns for compound suffix detection)
  - s_2 (3-character replacement string)
  - a_2 (array of 5 secondary suffix patterns)
- Called from (representative examples):
  - [danish_ISO_8859_1_stem](../d/danish_ISO_8859_1_stem.md) (main Danish stemming function)
  - [norwegian_ISO_8859_1_stem](../n/norwegian_ISO_8859_1_stem.md) (main Norwegian stemming function) 
  - [swedish_ISO_8859_1_stem](../s/swedish_ISO_8859_1_stem.md) (main Swedish stemming function)
  - [danish_UTF_8_stem](../d/danish_UTF_8_stem.md), norwegian_UTF_8_stem, swedish_UTF_8_stem (UTF-8 variants)

## Notes and Other Information
- This function runs after r_main_suffix to handle remaining morphological patterns
- The two-stage approach allows handling both exact compound patterns and general suffix categories
- Case 1 includes automatic consonant pair cleanup, showing the integrated nature of the stemming process
- Case 2 performs suffix replacement rather than deletion, handling morphological transformations
- The bit manipulation filtering (1572992 >> (z->p[z->c - 1] & 0x1f)) provides fast character-based exclusion
- Always returns 1 indicating successful processing (even if no changes were made)
- The test position mechanism (m1, m3) ensures cursor state is properly maintained across operations

## Simplified Source

```c
static int r_other_suffix(struct SN_env * z) {
    // Stage 1: Handle compound suffix patterns
    int saved_pos1 = z->l - z->c;
    z->ket = z->c;

    // Look for specific 2-character patterns s_0 and s_1
    if (eq_s_b(z, 2, s_0)) {
        z->bra = z->c;
        if (eq_s_b(z, 2, s_1)) {
            slice_del(z);  // Delete compound suffix
        }
    }
    z->c = z->l - saved_pos1;  // Restore position

    // Stage 2: Handle secondary suffix patterns
    if (z->c < z->I[1]) return 0;  // Check region boundary

    int saved_boundary = z->lb;
    z->lb = z->I[1];
    z->ket = z->c;

    // Quick character filter before pattern matching
    if (z->c - 1 <= z->lb || z->p[z->c - 1] >> 5 != 3 ||
        !((1572992 >> (z->p[z->c - 1] & 0x1f)) & 1)) {
        z->lb = saved_boundary;
        return 0;
    }

    // Try to match one of 5 secondary suffix patterns
    int suffix_match = find_among_b(z, a_2, 5);
    if (!suffix_match) {
        z->lb = saved_boundary;
        return 0;
    }

    z->bra = z->c;
    z->lb = saved_boundary;

    // Apply appropriate action based on matched pattern
    switch (suffix_match) {
        case 1:
            // Delete suffix and clean up doubled consonants
            slice_del(z);
            int saved_pos2 = z->l - z->c;
            r_consonant_pair(z);
            z->c = z->l - saved_pos2;
            break;
        case 2:
            // Replace suffix with 3-character string
            slice_from_s(z, 3, s_2);
            break;
    }

    return 1; // Success
}
```