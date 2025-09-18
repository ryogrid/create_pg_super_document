# danish_UTF_8_stem

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_danish.c:278-314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_danish.c#L278-L314)

## Overview
The main stemming function for Danish text in UTF-8 encoding that reduces words to their root forms by sequentially applying various suffix removal and morphological transformation rules.

## Definition


## Detailed Description
This function implements the complete Danish stemming algorithm as part of the Snowball stemming library. It processes a Danish word stored in the SN_env structure through a series of morphological transformations to reduce it to its stem form. The algorithm follows the standard Snowball stemming approach by:

1. First marking vowel/consonant regions in the word
2. Removing primary suffixes 
3. Handling doubled consonants after suffix removal
4. Removing secondary suffixes and applying additional morphological rules
5. Final undoubling of consonants at word end

The function operates on the word from right to left (end to beginning) and uses cursor positions to track processing state. Each transformation step is applied conditionally and can be rolled back if unsuccessful.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the word to be stemmed along with processing state including cursor positions, string boundaries, and working buffers

## Dependencies
- Functions called/Symbols referenced:
  - [r_mark_regions](../r/r_mark_regions.md): Identifies vowel/consonant regions for suffix boundary detection
  - [r_main_suffix](../r/r_main_suffix.md): Removes primary Danish suffixes 
  - [r_consonant_pair](../r/r_consonant_pair.md): Handles doubled consonant removal after suffixes
  - [r_other_suffix](../r/r_other_suffix.md): Processes secondary suffixes and morphological transformations
  - [r_undouble](../r/r_undouble.md): Final removal of doubled consonants at word end

- Called from (representative examples):
  - No direct references found (likely called via function pointer or external interface)

## Notes and Other Information
- Returns 1 on success, negative values on error
- The function carefully saves and restores cursor positions between operations using local variables (c1, m2-m5)
- Part of the generated Snowball stemmer code for Danish language support in PostgreSQL's full-text search
- The algorithm follows the standard Danish stemming rules as defined in the Snowball project
- Processing is done backwards from the end of the word (z->c = z->l) and then cursor is reset to beginning (z->c = z->lb)