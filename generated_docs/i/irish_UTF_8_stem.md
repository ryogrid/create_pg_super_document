# irish_UTF_8_stem

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_irish.c:432-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_irish.c#L432-L466)

## Overview
The irish_UTF_8_stem function is the main entry point for stemming Irish language text encoded in UTF-8, orchestrating the complete stemming process through multiple sequential phases.

## Definition

```c
}

extern int irish_UTF_8_stem(struct SN_env * z)
```
## Detailed Description
This function implements the complete Irish language stemming algorithm for UTF-8 encoded text. The stemming process follows a carefully structured sequence:

1. **Initial Morphological Processing**: Handles word-initial transformations and morphological variations
2. **Region Marking**: Identifies R1, R2, and RV boundaries within the word for morphological analysis
3. **Suffix Removal Phases**: Processes suffixes in three distinct phases:
   - **Noun Suffixes**: Removes nominal suffixes first
   - **Derivational Suffixes**: Handles derivational morphology transformations  
   - **Verb Suffixes**: Processes verbal suffixes last

Each suffix removal phase uses cursor position saving/restoration to ensure that if one phase fails, subsequent phases can still attempt processing from the original position. The algorithm processes the word from right-to-left (end-to-beginning) for suffix identification and removal.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the UTF-8 encoded word and stemming state
## Dependencies
- Functions called/Symbols referenced:
  - [r_initial_morph](../r/r_initial_morph.md): Handles initial morphological transformations
  - [r_mark_regions](../r/r_mark_regions.md): Identifies and marks R1, R2, and RV regions
  - [r_noun_sfx](../r/r_noun_sfx.md): Removes Irish noun suffixes
  - [r_deriv](../r/r_deriv.md): Processes derivational suffix transformations
  - [r_verb_sfx](../r/r_verb_sfx.md): Removes Irish verb suffixes
- Called from (representative examples):
  - This function appears to be an external API entry point for the Irish UTF-8 stemmer

## Notes and Other Information
- This is the UTF-8 variant of the Irish stemmer, with identical logic to the ISO-8859-1 version but supporting Unicode characters
- Returns 1 on successful completion, negative values on error
- The function is declared as 'extern', indicating it's part of the public API for the Irish stemmer
- Uses cursor position management (lb/c/l) to ensure proper word boundary handling
- The sequential suffix processing order (noun → derivational → verb) reflects the morphological structure of Irish words
- Each suffix removal phase is wrapped in position-saving blocks to allow independent processing attempts