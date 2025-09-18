# r_residual_form

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_portuguese.c:831-883](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_portuguese.c#L831-L883)

## Overview
The r_residual_form function handles specialized Portuguese morphological patterns involving specific character sequences and complex suffix transformations as a final step in the Portuguese stemming algorithm.

## Definition


## Detailed Description
The r_residual_form function is a Portuguese-specific stemming function that processes a small set of residual morphological patterns (a_8 array with 4 entries). It implements complex character-level analysis beyond simple suffix matching, examining specific letter combinations like 'gu' and 'ci' patterns. Case 1 performs sophisticated pattern matching: after initial suffix deletion, it checks for 'u' preceded by 'g' or 'i' preceded by 'c', performing additional deletions when these patterns are found within the RV region. Case 2 performs simple suffix replacement. This function handles Portuguese-specific morphological irregularities that require character-level analysis rather than simple pattern matching, making it unique among the stemming functions.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure (struct SN_env) containing:
  - : End position marker for the current match
  - : Current cursor position
  - : Beginning position marker for the current match
  - : Pointer to the word buffer
  - : Length of the word
  - : Left boundary of the word

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Pattern matching function for residual form identification
  - [r_RV](r_RV.md): RV region boundary checker (Portuguese uses RV instead of R1/R2 for some operations)
  - [slice_del](../s/slice_del.md): Suffix deletion function
  - [slice_from_s](../s/slice_from_s.md): Suffix replacement function
- Called from (representative examples):
  - [portuguese_ISO_8859_1_stem](../p/portuguese_ISO_8859_1_stem.md)
  - [portuguese_UTF_8_stem](../p/portuguese_UTF_8_stem.md)

## Notes and Other Information
- This function is specific to Portuguese stemming and demonstrates language-specific morphological complexity
- Uses RV region checking instead of R1/R2, which is characteristic of Portuguese stemming rules
- The complex character-level pattern matching in case 1 is unique among Snowball stemming functions
- Handles Portuguese orthographic patterns like 'gu' and 'ci' that require special morphological treatment
- Applied as one of the final steps in Portuguese stemming pipeline
- The s_10 string constant contains replacement text for case 2 transformations
- Demonstrates the need for character-level analysis in some morphologically complex languages
- Return value of 1 indicates successful processing, 0 indicates no residual form match found