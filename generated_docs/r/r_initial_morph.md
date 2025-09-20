# r_initial_morph

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_irish.c:257-317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_irish.c#L257-L317)

## Overview
The r_initial_morph function handles initial morphological transformations for Irish language stemming by processing word prefixes and applying appropriate substitutions or deletions.

## Definition

```c
}

static int r_initial_morph(struct SN_env * z)
```
## Detailed Description
This function implements the initial morphological processing step in the Irish Snowball stemming algorithm. It searches for specific patterns at the beginning of words using a predefined array (a_0 with 24 entries) and performs corresponding transformations. The function uses a switch statement to handle 10 different cases of morphological changes:

- Case 1: Deletes the matched prefix entirely
- Cases 2-10: Replace the matched prefix with predefined strings (s_0 through s_8)

The function sets bra and ket markers to delimit the portion of text being processed, then uses find_among to locate matching patterns. Based on the pattern found, it either deletes the matched text or substitutes it with a replacement string.

This preprocessing step is crucial for handling Irish initial mutations, lenition, and other morphophonemic changes that occur at word beginnings.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the word being processed and state information
  - : Beginning marker for the text slice to be modified
  - : Ending marker for the text slice to be modified
  - : Current cursor position in the word

## Dependencies
- Functions called/Symbols referenced:
  - [find_among](../f/find_among.md): Searches for patterns from array a_0 (24 entries) at current position
  - [slice_del](../s/slice_del.md): Deletes the text between bra and ket markers
  - [slice_from_s](../s/slice_from_s.md): Replaces text between bra and ket with a specified string
  - a_0: Array of 24 morphological patterns to match
  - s_0 through s_8: Replacement strings for various transformation cases
- Called from (representative examples):
  - [irish_ISO_8859_1_stem](../i/irish_ISO_8859_1_stem.md): Main Irish stemming function for ISO_8859_1 encoding
  - [irish_UTF_8_stem](../i/irish_UTF_8_stem.md): Main Irish stemming function for UTF_8 encoding

## Notes and Other Information
- Specific to Irish language morphology and handles initial consonant mutations
- Returns 0 if no pattern is matched, 1 on successful transformation
- Can return negative values if slice operations fail
- The a_0 array and s_0-s_8 strings contain language-specific Irish morphological rules
- This function is part of the preprocessing phase before main suffix removal rules are applied
- Essential for handling Irish linguistic features like lenition (softening of consonants) and eclipsis