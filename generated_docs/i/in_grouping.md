# in_grouping

## Location
src/backend/snowball/libstemmer/utilities.c: 167 - 178

## Overview
The `in_grouping` function checks if characters in a Snowball stemmer environment fall within a specified character grouping for non-UTF8 encodings.

## Definition
```c
extern int in_grouping(struct SN_env * z, const unsigned char * s, int min, int max, int repeat)
```

## Detailed Description
This function is part of the Snowball stemming library utilities for character groupings in non-UTF8 cases. It examines characters at the current position in the string buffer and determines if they belong to a specified character group defined by a bitmask. The function can operate in two modes: single character check or repeated character checking until a non-matching character is found.

The function uses a bitmask approach where each bit in the array `s` represents whether a character (relative to `min`) is included in the grouping. Characters are checked against the range [min, max] and their corresponding bit in the bitmask.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing the string buffer and current position
- `s`: Unsigned char array containing the bitmask defining which characters are in the grouping
- `min`: Minimum character value in the grouping range
- `max`: Maximum character value in the grouping range  
- `repeat`: Boolean flag indicating whether to continue checking consecutive characters (1) or check only one character (0)

## Dependencies
- Functions called/Symbols referenced:
  - [SN_env](../S/SN_env.md) struct members (z->c, z->l, z->p)
  - [repeat](../r/repeat.md) parameter
- Called from (representative examples):
  - [r_mark_regions](../r/r_mark_regions.md) (in various language stemmers)
  - [r_prelude](../r/r_prelude.md) (in Dutch, English, French, German, Italian, Romanian stemmers)
  - [porter_ISO_8859_1_stem](../p/porter_ISO_8859_1_stem.md) (in Porter stemmer)
  - [r_VOWEL](../r/r_VOWEL.md) (in Indonesian stemmer)

## Notes and Other Information
- Returns -1 if at end of string buffer (z->c >= z->l)
- Returns 1 if character is not in the specified grouping
- Returns 0 if character(s) successfully match the grouping
- The bitmask uses bit operations: `(s[ch >> 3] & (0X1 << (ch & 0X7)))` to check if a character is in the group
- Used extensively across multiple language-specific Snowball stemmers for vowel/consonant detection and region marking
- Character position (z->c) is incremented for each matching character when repeat is enabled