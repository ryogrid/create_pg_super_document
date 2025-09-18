# irish_ISO_8859_1_stem

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_irish.c:432-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_irish.c#L432-L466)

## Overview
This is the main entry point function for Irish language stemming using ISO 8859-1 character encoding, orchestrating the complete stemming process through multiple morphological analysis stages.

## Definition


## Detailed Description
The `irish_ISO_8859_1_stem` function is the primary stemming algorithm for Irish text processing. It implements the complete Irish stemming algorithm through a carefully orchestrated sequence of morphological transformations:

1. **Initial Morphological Processing**: Calls `r_initial_morph` to handle initial consonant mutations and other Irish-specific word transformations, then resets the cursor position to preserve the original processing point.

2. **Region Marking**: Uses `r_mark_regions` to identify linguistic boundaries (R1, R2, and RV regions) that will guide suffix removal decisions.

3. **Suffix Processing Pipeline**: Processes three types of suffixes in order:
   - **Noun suffixes** (`r_noun_sfx`): Removes common Irish noun endings like -íocht, -ire, -abh, etc.
   - **Derivational suffixes** (`r_deriv`): Handles complex derivational patterns with both deletion and transformation operations
   - **Verb suffixes** (`r_verb_sfx`): Removes Irish verb endings like -imid, -adh, -áil, etc.

4. **Cursor Management**: Uses save/restore cursor positions (m2, m3, m4) to ensure that each suffix processing stage operates independently without interfering with subsequent stages.

The function processes text from right to left (standard Snowball approach) and resets to the left boundary (`z->lb`) before completion.

## Parameters / Member Variables
- `z`: Pointer to the SN_env (Snowball environment) structure containing:
  - Text buffer with the word to be stemmed
  - Cursor positions (c, lb, l) for tracking processing position
  - Regional boundaries (I[0], I[1], I[2]) set by r_mark_regions

## Dependencies
- Functions called/Symbols referenced:
  - [r_initial_morph](../r/r_initial_morph.md) (handles Irish initial consonant mutations)
  - [r_mark_regions](../r/r_mark_regions.md) (identifies R1, R2, and RV linguistic regions)
  - [r_noun_sfx](../r/r_noun_sfx.md) (removes noun suffixes)
  - [r_deriv](../r/r_deriv.md) (processes derivational suffixes with transformations)
  - [r_verb_sfx](../r/r_verb_sfx.md) (removes verb suffixes)
- Called from:
  - External stemming interfaces (as this is an extern function)
  - PostgreSQL text search subsystem for Irish language processing

## Notes and Other Information
- This function is part of the Snowball stemming algorithm library integrated into PostgreSQL
- The extern declaration makes it available to other modules for Irish text processing
- The function is designed to handle ISO 8859-1 encoded Irish text (Latin-1 with Irish characters)
- There is also a corresponding UTF-8 variant (irish_UTF_8_stem) with identical logic
- The cursor position management ensures that each suffix type is considered independently
- Returns 1 on successful completion, negative values indicate errors during processing
- The order of suffix processing (noun → derivational → verb) reflects linguistic priorities in Irish morphology
- This implementation follows the Irish stemming rules defined in the Snowball algorithm specification