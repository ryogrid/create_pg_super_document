# in_grouping_b

## Location
src/backend/snowball/libstemmer/utilities.c: 179 - 190

## Overview
The `in_grouping_b` function checks if characters in a Snowball stemmer environment fall within a specified character grouping, operating in backward direction for non-UTF8 encodings.

## Definition
```c
extern int in_grouping_b(struct SN_env * z, const unsigned char * s, int min, int max, int repeat)
```

## Detailed Description
This function is the backward counterpart to `in_grouping`, part of the Snowball stemming library utilities for character groupings in non-UTF8 cases. It examines characters moving backward from the current position in the string buffer and determines if they belong to a specified character group defined by a bitmask. The function can operate in two modes: single character check or repeated character checking until a non-matching character is found.

Like `in_grouping`, it uses a bitmask approach where each bit in the array `s` represents whether a character (relative to `min`) is included in the grouping. The key difference is that this function moves backward through the string (decrements z->c) and checks against the left boundary (z->lb).

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing the string buffer and current position
- `s`: Unsigned char array containing the bitmask defining which characters are in the grouping
- `min`: Minimum character value in the grouping range
- `max`: Maximum character value in the grouping range
- `repeat`: Boolean flag indicating whether to continue checking consecutive characters backward (1) or check only one character (0)

## Dependencies
- Functions called/Symbols referenced:
  - [SN_env](../S/SN_env.md) struct members (z->c, z->lb, z->p)
  - [repeat](../r/repeat.md) parameter
- Called from (representative examples):
  - [r_main_suffix](../r/r_main_suffix.md) (in Danish, Norwegian, Swedish stemmers)
  - [r_shortv](../r/r_shortv.md) (in English and Porter stemmers)
  - [r_standard_suffix](../r/r_standard_suffix.md) (in French and German stemmers)
  - [r_vowel_suffix](../r/r_vowel_suffix.md) (in Italian stemmer)
  - Various language-specific suffix and tidy functions

## Notes and Other Information
- Returns -1 if at left boundary of string buffer (z->c <= z->lb)
- Returns 1 if character is not in the specified grouping
- Returns 0 if character(s) successfully match the grouping
- Accesses characters at position z->c - 1 (one position back from current)
- Character position (z->c) is decremented for each matching character when repeat is enabled
- Used extensively for backward pattern matching in suffix removal and vowel/consonant detection
- Essential for stemming algorithms that need to analyze word endings by moving backward from the current position