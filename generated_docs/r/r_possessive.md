# r_possessive

## Location
src/backend/snowball/libstemmer/stem_UTF_8_finnish.c: 332 - 397

## Overview
The r_possessive function removes Finnish possessive suffixes from words during the stemming process, handling the complex morphological transformations required for Finnish possessive markers.

## Definition
static int r_possessive(struct SN_env * z)

## Detailed Description
This function implements the Finnish possessive suffix removal step in the Snowball stemming algorithm. Finnish has a rich possessive system with suffixes that indicate the person and number of the possessor (my, your, his/her, our, your plural, their). The function identifies and removes these possessive markers while applying necessary morphological transformations.

The algorithm operates within the R1 region boundary and uses a multi-case switch structure to handle different types of possessive suffixes:

- **Case 1**: Handles possessive suffixes that should not be removed if preceded by 'k'
- **Case 2**: Removes possessive and replaces a specific pattern (s_0) with another (s_1)
- **Case 3**: Simple possessive suffix removal
- **Cases 4-6**: Handle possessive suffixes with specific vowel requirements:
  - Case 4: Requires preceding vowel 'a' (character 97)
  - Case 5: Requires preceding vowel 'ä' (character 228)
  - Case 6: Requires preceding vowel 'e' (character 101) with additional pattern matching

Each case performs pattern matching against different suffix arrays (a_1, a_2, a_3, a_4) to ensure accurate identification of possessive markers.

## Parameters / Member Variables
- : Pointer to SN_env structure containing:
  - : Current cursor position
  - : R1 region boundary marker
  - : Left boundary for processing
  - : End position of matched substring
  - : Start position of matched substring
  - : Length of the string
  - : Pointer to the string data
- : Local variable storing the type of possessive suffix found (1-6)
- : Local variable storing the original left boundary
- : Local variable for position tracking

## Dependencies
- Functions called/Symbols referenced:
  - find_among_b: Backward pattern matching function
  - eq_s_b: Backward string equality test
  - slice_del: Function to delete matched substring
  - slice_from_s: Function to replace substring with new content
  - a_1, a_2, a_3, a_4: Arrays containing possessive suffix patterns
  - s_0, s_1: String constants for pattern replacement
- Called from (representative examples):
  - finnish_ISO_8859_1_stem: Main Finnish stemming function
  - finnish_UTF_8_stem: UTF-8 version of Finnish stemming

## Notes and Other Information
This function is highly specific to Finnish morphology, reflecting the complex possessive system of the Finnish language. Finnish possessive suffixes can trigger vowel harmony changes and consonant gradation, which is why the function includes multiple cases with vowel-specific requirements. The function handles both simple removal and morphological transformations (case 2 with replacement). The character codes used (97='a', 228='ä', 101='e') reflect the ISO-8859-1 encoding for Finnish vowels. This function typically runs after particle removal but before case suffix processing in the Finnish stemming pipeline.