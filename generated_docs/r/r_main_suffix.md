# r_main_suffix

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_danish.c:158-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_danish.c#L158-L186)

## Overview
This function removes primary suffixes from Danish words as part of the Snowball stemming algorithm, using pattern matching to identify and delete appropriate word endings.

## Definition

```c
}

static int r_main_suffix(struct SN_env * z)
```
## Detailed Description
The r_main_suffix function implements the main suffix removal step in the Danish stemming algorithm. It operates by:

1. Checking if the current position is within the valid region (beyond I[1] boundary)
2. Setting up a temporary boundary limit to constrain the search
3. Using pattern matching to identify known Danish suffixes from a predefined list (a_0 with 32 entries)
4. Performing character-level filtering to quickly eliminate non-matching candidates
5. Executing suffix removal based on two different cases:
   - Case 1: Simple deletion of the matched suffix
   - Case 2: Conditional deletion requiring the preceding character to be in the s_ending group

The function uses backward searching (find_among_b) to match suffixes from the end of the word, which is typical for suffix-based morphological analysis.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the word being processed
  - : Current cursor position in the word
  - : Region boundary marker (from r_mark_regions)
  - : Left boundary limit
  - : End position of matched substring
  - : Beginning position of matched substring
  - : Pointer to the word being processed

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward pattern matching against suffix list a_0)
  - [slice_del](../s/slice_del.md) (suffix deletion operation)
  - [in_grouping_b](../i/in_grouping_b.md) (backward character group checking for g_s_ending, characters 97-229)
  - a_0 (array of 32 Danish suffix patterns)
  - g_s_ending (character grouping for s-ending validation)
- Called from (representative examples):
  - [danish_ISO_8859_1_stem](../d/danish_ISO_8859_1_stem.md)
  - [norwegian_ISO_8859_1_stem](../n/norwegian_ISO_8859_1_stem.md)
  - [swedish_ISO_8859_1_stem](../s/swedish_ISO_8859_1_stem.md)
  - [danish_UTF_8_stem](../d/danish_UTF_8_stem.md)
  - [norwegian_UTF_8_stem](../n/norwegian_UTF_8_stem.md)
  - [swedish_UTF_8_stem](../s/swedish_UTF_8_stem.md)

## Notes and Other Information
- This function is used in Scandinavian language stemmers (Danish, Norwegian, Swedish)
- The bit manipulation (>> 5, & 0x1f, 1851440) provides fast character filtering before expensive pattern matching
- Returns 0 if no suffix is found or conditions aren't met, 1 if successful
- The among_var determines which deletion rule to apply based on the matched suffix
- Case 2 adds an additional constraint requiring specific preceding characters (s_ending group)
- The temporary boundary manipulation ensures suffix matching occurs only in appropriate word regions

## Simplified Source

```c
static int r_main_suffix(struct SN_env * z) {
    // Ensure we're in the correct region (beyond R1 boundary)
    if (z->c < z->I[1]) return 0;

    // Set up boundary limits for suffix matching
    int saved_boundary = z->lb;
    z->lb = z->I[1];
    z->ket = z->c;

    // Quick character filter before expensive pattern matching
    if (z->c <= z->lb || z->p[z->c - 1] >> 5 != 3 ||
        !((1851440 >> (z->p[z->c - 1] & 0x1f)) & 1)) {
        z->lb = saved_boundary;
        return 0;
    }

    // Try to match one of 32 Danish suffix patterns
    int suffix_match = find_among_b(z, a_0, 32);
    if (!suffix_match) {
        z->lb = saved_boundary;
        return 0;
    }

    z->bra = z->c;
    z->lb = saved_boundary;

    // Apply appropriate deletion rule
    switch (suffix_match) {
        case 1:
            // Simple suffix deletion
            slice_del(z);
            break;
        case 2:
            // Conditional deletion - check for s-ending character
            if (in_grouping_b(z, g_s_ending, 97, 229, 0)) return 0;
            slice_del(z);
            break;
    }

    return 1; // Success
}
```