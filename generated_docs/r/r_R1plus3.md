# r_R1plus3

## Location
src/backend/snowball/libstemmer/stem_UTF_8_yiddish.c: 736 - 740

## Overview
The r_R1plus3 function tests whether the current cursor position plus 3 characters (encoded as 6 bytes in UTF-8) is within the R1 region boundary, used specifically in Yiddish stemming rules.

## Definition


## Detailed Description
The r_R1plus3 function is a specialized boundary checking function specific to the Yiddish Snowball stemming algorithm. It performs a more restrictive test than the standard r_R1 function by requiring that not only the current cursor position, but also the position 3 characters ahead (represented as 6 bytes due to UTF-8 encoding considerations) must be within the R1 morphological region.

This function ensures that there is sufficient character space within the R1 region to safely apply certain suffix removal operations that might affect multiple characters. The +6 offset accounts for UTF-8 encoding where some characters may require multiple bytes, making this a UTF-8 aware boundary check.

The function returns 1 (true) if the R1 boundary position is less than or equal to the cursor position plus 6 bytes, indicating sufficient space for the operation, and 0 (false) otherwise.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure (SN_env) containing:
  - : Current cursor position in the UTF-8 encoded string
  - : Integer array element storing the R1 region boundary position (set by r_mark_regions)

## Dependencies
- Functions called/Symbols referenced:
  - None (performs direct arithmetic comparison only)

- Called from (representative examples):
  -  (Yiddish suffix processing)

## Notes and Other Information
- This function is specific to the Yiddish stemming implementation and reflects language-specific morphological requirements
- The +6 byte offset accounts for UTF-8 encoding considerations where Yiddish characters may require multiple bytes
- Only used within the Yiddish stemmer, unlike the general r_R1 function which is used across all languages
- Provides a more conservative boundary check to prevent over-stemming in Yiddish morphology
- The function is stateless and has no side effects
- Must be called after r_mark_regions has established the R1 boundary