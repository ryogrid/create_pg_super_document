# in_grouping_b_U

## Location
src/backend/snowball/libstemmer/utilities.c: 129 - 140

## Overview
Checks if the character preceding the current cursor position belongs to a specified character group, moving the cursor backward if a match is found.

## Definition
extern int in_grouping_b_U(struct SN_env * z, const unsigned char * s, int min, int max, int repeat)

## Detailed Description
This function is the backward counterpart to in_grouping_U, performing character group membership testing while moving backward through the text. It uses backward UTF-8 decoding to read characters from right to left, which is essential for suffix-based stemming operations where the algorithm needs to process word endings.

The function operates by:
1. Decoding the UTF-8 character immediately before the current cursor position using get_b_utf8
2. Checking if the character falls within the specified min-max range
3. Using the character as an index into the bitmap to test group membership
4. Moving the cursor backward if the character is in the group
5. Optionally repeating the process for multiple consecutive characters

This backward processing capability is crucial for stemming algorithms that need to identify and process suffixes, prefixes in reverse, or other patterns that require right-to-left text analysis.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the text and cursor state
- : Bitmap representing the character group (each bit indicates group membership)
- : Minimum character value in the group range
- : Maximum character value in the group range
- : Flag indicating whether to continue matching consecutive characters in the group

## Dependencies
- Functions called/Symbols referenced:
  - get_b_utf8 (for backward UTF-8 character decoding)
  - repeat (parameter used in control flow)
- Called from (representative examples):
  - r_main_suffix (in various language stemmers)
  - r_shortv (in English and Porter stemmers)
  - r_Step_2 (in English stemmer)
  - r_standard_suffix (in French, German, Yiddish stemmers)

## Notes and Other Information
- Returns 0 on successful completion when repeat is true and all consecutive characters match
- Returns the byte width of the non-matching character when a character is not in the group
- Returns -1 if backward UTF-8 decoding fails
- The bitmap testing uses the same bit manipulation as in_grouping_U: checking group membership via bit operations
- Cursor movement is backward (z->c -= w) unlike the forward movement in in_grouping_U
- Essential for suffix processing and other backward text analysis in stemming algorithms
- Used extensively in morphological analysis where suffixes and endings need to be identified and processed
- The function is declared as extern, making it available to generated stemmer code