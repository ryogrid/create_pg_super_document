# finnish_UTF_8_stem

## Location
src/backend/snowball/libstemmer/stem_UTF_8_finnish.c: 658 - 718

## Overview
The finnish_UTF_8_stem function is the main entry point for performing Finnish language stemming on UTF-8 encoded text, implementing the complete Finnish stemming algorithm by orchestrating various morphological analysis steps.

## Definition
extern int finnish_UTF_8_stem(struct SN_env * z)

## Detailed Description
The finnish_UTF_8_stem function implements the complete Finnish stemming algorithm for UTF-8 encoded text. It follows a systematic approach to morphological analysis, processing different types of Finnish word endings in a specific order to reduce words to their base forms.

The stemming process follows these sequential steps:
1. **Region marking**: Establishes morphological boundaries (R1, R2, RV regions)
2. **Particle removal**: Removes particles and clitics (r_particle_etc)
3. **Possessive suffix removal**: Handles possessive endings (r_possessive)  
4. **Case ending removal**: Processes grammatical case suffixes (r_case_ending)
5. **Other endings removal**: Handles miscellaneous morphological endings (r_other_endings)
6. **Plural handling**: Conditionally processes either i-plurals or t-plurals based on previous analysis
7. **Final cleanup**: Normalizes the result with character pattern cleanup (r_tidy)

The function uses backward processing (from end to beginning) and employs conditional branching to handle different plural forms based on the I[2] flag set during earlier processing stages.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the UTF-8 encoded word to be stemmed and all stemming state information

## Dependencies
- Functions called/Symbols referenced:
  - [r_mark_regions](../r/r_mark_regions.md)
  - [r_particle_etc](../r/r_particle_etc.md)
  - [r_possessive](../r/r_possessive.md)
  - [r_case_ending](../r/r_case_ending.md)
  - [r_other_endings](../r/r_other_endings.md)
  - [r_i_plural](../r/r_i_plural.md)
  - [r_t_plural](../r/r_t_plural.md)
  - [r_tidy](../r/r_tidy.md)
- Called from (representative examples):
  - (External callers - this is a public interface function)

## Notes and Other Information
- This is the main public interface for Finnish UTF-8 stemming in the PostgreSQL Snowball implementation
- Always returns 1 on successful completion, or negative values on error conditions
- The I[2] flag controls whether i-plural or t-plural processing is performed, preventing conflicts between different plural forms
- Designed specifically for UTF-8 encoding, with a parallel ISO-8859-1 version available for Latin-1 text
- Each morphological step is attempted but failures don't prevent subsequent processing steps
- The function resets the cursor position after each step to allow independent processing of different morphological layers