# r_verb_sfx

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_irish.c:404-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_irish.c#L404-L431)

## Overview
The r_verb_sfx function removes Irish verb suffixes during the stemming process, with different removal strategies based on RV and R1 regions and includes character-based pre-filtering for efficiency.

## Definition

```c
}

static int r_verb_sfx(struct SN_env * z)
```
## Detailed Description
This function handles Irish verb suffix removal as part of the stemming algorithm. It includes several distinctive features:

1. **Character-based pre-filtering**: Before attempting suffix matching, it performs a bit-pattern check on the character before the cursor position to quickly eliminate words that cannot contain valid verb suffixes
2. **Dual region-based removal**: Uses both RV (vowel region) and R1 (first morphological region) boundaries for different suffix types
3. **Two removal strategies**: 
   - **Case 1 suffixes**: Removed if they occur within the RV region
   - **Case 2 suffixes**: Removed if they occur within the R1 region

The pre-filtering optimization (using bit pattern 282896) helps avoid expensive suffix table lookups when the word structure indicates no valid verb suffix can be present.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the word being processed and stemming state information
## Dependencies
- Functions called/Symbols referenced:
  - [r_RV](r_RV.md): Checks if current position is within RV (vowel) region
  - [r_R1](r_R1.md): Checks if current position is within R1 region
  - [find_among_b](../f/find_among_b.md): Searches backwards for matching verb suffix patterns
  - [slice_del](../s/slice_del.md): Deletes the identified suffix from the word
- Called from (representative examples):
  - [irish_ISO_8859_1_stem](../i/irish_ISO_8859_1_stem.md): Main stemming function for ISO-8859-1 encoded Irish text
  - [irish_UTF_8_stem](../i/irish_UTF_8_stem.md): Main stemming function for UTF-8 encoded Irish text

## Notes and Other Information
- The function uses lookup table 'a_3' containing 12 different Irish verb suffix patterns
- Returns 1 on successful suffix removal, 0 if no suffix found or pre-filter fails, or error code if operation fails
- The bit pattern check (282896 >> (character & 0x1f)) provides fast rejection of impossible verb suffix candidates
- Uses both RV and R1 regions, making it more flexible than functions that only use R1/R2 boundaries
- This function is called after derivational suffix processing in the overall Irish stemming algorithm

## Simplified Source

```c
static int r_verb_sfx(struct SN_env * z) {
    // Set boundary for verb suffix search
    z->ket = z->c;

    // Quick character-based pre-filter for efficiency
    if (z->c - 2 <= z->lb || z->p[z->c - 1] >> 5 != 3 ||
        !((282896 >> (z->p[z->c - 1] & 0x1f)) & 1))
        return 0;

    // Find verb suffix pattern
    int among_var = find_among_b(z, a_3, 12);
    if (!among_var) return 0;

    z->bra = z->c;

    // Remove suffix based on morphological region
    switch (among_var) {
        case 1:
            // Delete if in RV (vowel) region
            if (r_RV(z) > 0) slice_del(z);
            break;
        case 2:
            // Delete if in R1 region
            if (r_R1(z) > 0) slice_del(z);
            break;
    }
    return 1;
}
```