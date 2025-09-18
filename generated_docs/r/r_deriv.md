# r_deriv

## Location
src/backend/snowball/libstemmer/stem_UTF_8_irish.c: 360 - 403

## Overview
The r_deriv function handles Irish derivational suffix removal and transformation during the stemming process, performing both deletion and replacement operations based on the identified suffix type.

## Definition


## Detailed Description
This function processes Irish derivational suffixes that modify the meaning or grammatical category of root words. It uses a lookup table (a_2 with 25 entries) to identify derivational suffix patterns and applies one of six different transformation strategies:

1. **Case 1**: Simple deletion if the suffix occurs within the R2 region
2. **Cases 2-6**: Replacement transformations where suffixes are replaced with specific character sequences (s_9, s_10, s_11, s_12, s_13)

The replacement operations are particularly important in Irish morphology as they often restore original word forms that have been modified through derivational processes. The function follows the standard Snowball pattern of setting ket/bra boundaries around the identified suffix.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the word being processed and stemming state information

## Dependencies
- Functions called/Symbols referenced:
  - [r_R2](r_R2.md): Checks if current position is within R2 region
  - [find_among_b](../f/find_among_b.md): Searches backwards for matching derivational suffix patterns
  - [slice_del](../s/slice_del.md): Deletes the identified suffix from the word
  - [slice_from_s](../s/slice_from_s.md): Replaces the suffix with a specific character sequence
- Called from (representative examples):
  - [irish_ISO_8859_1_stem](../i/irish_ISO_8859_1_stem.md): Main stemming function for ISO-8859-1 encoded Irish text
  - [irish_UTF_8_stem](../i/irish_UTF_8_stem.md): Main stemming function for UTF-8 encoded Irish text

## Notes and Other Information
- The function uses lookup table 'a_2' containing 25 different Irish derivational suffix patterns
- Returns 1 on successful operation, 0 if no suffix found, or error code if operation fails
- The replacement strings (s_9 through s_13) contain specific Irish character sequences that restore morphological forms
- Only Case 1 requires R2 region checking; replacement cases (2-6) apply unconditionally when the suffix is found
- This function is called after noun suffix processing in the overall Irish stemming algorithm