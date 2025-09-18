# r_noun_sfx

## Location
src/backend/snowball/libstemmer/stem_UTF_8_irish.c: 333 - 359

## Overview
The r_noun_sfx function removes Irish noun suffixes during the stemming process, handling both R1 and R2 region-based suffix removal according to Irish morphological rules.

## Definition


## Detailed Description
This function is part of the Irish language stemmer and is responsible for removing noun suffixes from Irish words. The function uses a lookup table (a_1 with 16 entries) to identify valid noun suffixes at the end of the word. Based on the suffix type found, it applies different removal strategies:

1. **Case 1 suffixes**: Removed if they occur within the R1 region (first morphological boundary)
2. **Case 2 suffixes**: Removed if they occur within the R2 region (second morphological boundary, more restrictive)

The function follows the standard Snowball stemmer pattern of setting boundary markers (ket/bra) around the identified suffix before attempting removal.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the word being processed and stemming state information

## Dependencies
- Functions called/Symbols referenced:
  - r_R1: Checks if current position is within R1 region
  - r_R2: Checks if current position is within R2 region
  - find_among_b: Searches backwards for matching suffix patterns
  - slice_del: Deletes the identified suffix from the word
- Called from (representative examples):
  - irish_ISO_8859_1_stem: Main stemming function for ISO-8859-1 encoded Irish text
  - irish_UTF_8_stem: Main stemming function for UTF-8 encoded Irish text

## Notes and Other Information
- The function uses a lookup table 'a_1' containing 16 different Irish noun suffix patterns
- Returns 1 on successful suffix removal, 0 if no suffix found, or error code if operation fails
- The distinction between R1 and R2 regions ensures that suffixes are only removed when they appear in linguistically appropriate contexts
- This function is called as part of the overall Irish stemming algorithm after initial morphological processing