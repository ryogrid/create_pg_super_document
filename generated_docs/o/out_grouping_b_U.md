# out_grouping_b_U

## Location
src/backend/snowball/libstemmer/utilities.c: 153 - 166

## Overview
Moves the cursor backward while characters do NOT belong to a specified character group, stopping when a character in the group is found.

## Definition
extern int out_grouping_b_U(struct SN_env * z, const unsigned char * s, int min, int max, int repeat)

## Detailed Description
This function is the backward counterpart to out_grouping_U, combining backward text traversal with inverse character group testing. It moves the cursor backward through the text, skipping over characters that are NOT members of the specified character group, and stops when it encounters a character that IS in the group. This functionality is crucial for backward pattern matching and suffix analysis in stemming algorithms.

The function operates by:
1. Using backward UTF-8 decoding to read the character immediately before the current cursor position
2. Checking if the character falls within the specified min-max range and testing group membership
3. If the character is NOT in the group, moving the cursor backward and continuing
4. If the character IS in the group, stopping and returning the character's byte width
5. Optionally repeating the process to skip over consecutive backward non-group characters

This is particularly valuable for operations like finding the beginning of vowel sequences when processing from right to left, locating morphological boundaries in suffixes, or identifying transitions between character classes in reverse order.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the text and cursor state
- : Bitmap representing the character group (each bit indicates group membership)
- : Minimum character value in the group range
- : Maximum character value in the group range
- : Flag indicating whether to continue skipping consecutive non-group characters

## Dependencies
- Functions called/Symbols referenced:
  - get_b_utf8 (for backward UTF-8 character decoding)
  - repeat (parameter used in control flow)
- Called from (representative examples):
  - r_shortv (in English and Porter stemmers)
  - r_Step_1a, r_Step_1b, r_Step_1c (in English stemmer)
  - r_standard_suffix (in Dutch and French stemmers)
  - r_check_vowel_harmony (in Turkish stemmer)

## Notes and Other Information
- Returns 0 on successful completion when repeat is true and all characters processed are outside the group
- Returns the byte width of the first character found that IS in the group (when moving backward)
- Returns -1 if backward UTF-8 decoding fails
- Uses inverted logic combined with backward movement: continues while characters are NOT in the group
- Cursor movement is backward (z->c -= w) and stops when a target character is found
- Essential for suffix analysis and backward pattern matching in morphological processing
- Particularly important in languages with complex vowel harmony rules (like Turkish)
- Used extensively for checking conditions before applying morphological transformations
- The function is declared as extern, making it available to generated stemmer code
- Critical for implementing short vowel detection and other backward linguistic pattern recognition