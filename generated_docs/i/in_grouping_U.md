# in_grouping_U

## Location
[src/backend/snowball/libstemmer/utilities.c:117-128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L117-L128)

## Overview
Checks if the current UTF-8 character at the cursor position belongs to a specified character group, advancing the cursor if a match is found.

## Definition
extern int in_grouping_U(struct SN_env * z, const unsigned char * s, int min, int max, int repeat)

## Detailed Description
This function is a core utility in the Snowball stemming algorithm that tests whether the current UTF-8 character in the input string belongs to a defined character group. The character group is represented as a bitmap where each bit indicates whether a character (relative to a minimum value) is part of the group. The function can operate in two modes: single character check or repeated matching.

The function works by:
1. Decoding the UTF-8 character at the current cursor position
2. Checking if the character falls within the specified min-max range
3. Using the character as an index into the bitmap to test group membership
4. Advancing the cursor if the character is in the group
5. Optionally repeating the process for multiple consecutive characters

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the text and cursor state
- : Bitmap representing the character group (each bit indicates group membership)
- : Minimum character value in the group range
- : Maximum character value in the group range  
- : Flag indicating whether to continue matching consecutive characters in the group

## Dependencies
- Functions called/Symbols referenced:
  - [get_utf8](../g/get_utf8.md) (for UTF-8 character decoding)
  - [repeat](../r/repeat.md) (parameter used in control flow)
- Called from (representative examples):
  - [r_mark_regions](../r/r_mark_regions.md) (in various language stemmers)
  - [r_prelude](../r/r_prelude.md) (in various language stemmers)
  - [porter_UTF_8_stem](../p/porter_UTF_8_stem.md)
  - [lithuanian_UTF_8_stem](../l/lithuanian_UTF_8_stem.md)

## Notes and Other Information
- Returns 0 on successful completion when repeat is true and all consecutive characters match
- Returns the byte width of the non-matching character when a character is not in the group
- Returns -1 if UTF-8 decoding fails
- The bitmap uses bit manipulation:  to test membership
- Used extensively across all UTF-8 language stemmers in PostgreSQL's text search
- Essential for identifying vowels, consonants, and other character classes in stemming algorithms
- The function is declared as extern, making it available to generated stemmer code