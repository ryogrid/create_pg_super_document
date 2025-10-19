# r_noun_sfx

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_irish.c:333-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_irish.c#L333-L359)

## Overview
The r_noun_sfx function removes Irish noun suffixes during the stemming process, handling both R1 and R2 region-based suffix removal according to Irish morphological rules.

## Definition

```c
}

static int r_noun_sfx(struct SN_env * z)
```
## Detailed Description
This function is part of the Irish language stemmer and is responsible for removing noun suffixes from Irish words. The function uses a lookup table (a_1 with 16 entries) to identify valid noun suffixes at the end of the word. Based on the suffix type found, it applies different removal strategies:

1. **Case 1 suffixes**: Removed if they occur within the R1 region (first morphological boundary)
2. **Case 2 suffixes**: Removed if they occur within the R2 region (second morphological boundary, more restrictive)

The function follows the standard Snowball stemmer pattern of setting boundary markers (ket/bra) around the identified suffix before attempting removal.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the word being processed and stemming state information
## Dependencies
- Functions called/Symbols referenced:
  - [r_R1](r_R1.md): Checks if current position is within R1 region
  - [r_R2](r_R2.md): Checks if current position is within R2 region
  - [find_among_b](../f/find_among_b.md): Searches backwards for matching suffix patterns
  - [slice_del](../s/slice_del.md): Deletes the identified suffix from the word
- Called from (representative examples):
  - [irish_ISO_8859_1_stem](../i/irish_ISO_8859_1_stem.md): Main stemming function for ISO-8859-1 encoded Irish text
  - [irish_UTF_8_stem](../i/irish_UTF_8_stem.md): Main stemming function for UTF-8 encoded Irish text

## Notes and Other Information
- The function uses a lookup table 'a_1' containing 16 different Irish noun suffix patterns
- Returns 1 on successful suffix removal, 0 if no suffix found, or error code if operation fails
- The distinction between R1 and R2 regions ensures that suffixes are only removed when they appear in linguistically appropriate contexts
- This function is called as part of the overall Irish stemming algorithm after initial morphological processing

## Simplified Source

```c
static int r_noun_sfx(struct SN_env * z) {
    // Set boundary and find noun suffix pattern
    z->ket = z->c;
    int among_var = find_among_b(z, a_1, 16);
    if (!among_var) return 0;

    z->bra = z->c;

    // Remove suffix based on morphological region
    switch (among_var) {
        case 1:
            // Delete if in R1 region (first morphological boundary)
            if (r_R1(z) > 0) slice_del(z);
            break;
        case 2:
            // Delete if in R2 region (more restrictive boundary)
            if (r_R2(z) > 0) slice_del(z);
            break;
    }
    return 1;
}
```