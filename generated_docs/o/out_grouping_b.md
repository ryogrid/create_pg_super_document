# out_grouping_b

## Location
[src/backend/snowball/libstemmer/utilities.c:203-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L203-L214)

## Overview
The `out_grouping_b` function checks if characters in a Snowball stemmer environment fall outside a specified character grouping, operating in backward direction for non-UTF8 encodings.

## Definition
```c
extern int out_grouping_b(struct SN_env * z, const unsigned char * s, int min, int max, int repeat)
```

## Detailed Description
This function combines the backward movement logic of `in_grouping_b` with the inverted matching logic of `out_grouping`. It is part of the Snowball stemming library utilities for character groupings in non-UTF8 cases. The function examines characters moving backward from the current position and determines if they fall outside a specified character group defined by a bitmask. It can operate in two modes: single character check or repeated character checking until a matching character (one that is in the grouping) is found.

Like its forward counterpart, it uses inverted logic - returning 1 when a character IS in the grouping and continuing when characters are NOT in the grouping, but operates by moving backward through the string.

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
  - [r_shortv](../r/r_shortv.md) (in English and Porter stemmers)
  - [r_standard_suffix](../r/r_standard_suffix.md) (in Dutch and French stemmers)
  - [r_Step_1a](../r/r_Step_1a.md), r_Step_1b, r_Step_1c (in English and Porter stemmers)
  - [r_main_suffix](../r/r_main_suffix.md) (in Norwegian stemmer)
  - Various ending and suffix processing functions

## Notes and Other Information
- Returns -1 if at left boundary of string buffer (z->c <= z->lb)
- Returns 1 if character is found within the specified grouping
- Returns 0 if character(s) successfully stay outside the grouping
- Accesses characters at position z->c - 1 (one position back from current)
- Character position (z->c) is decremented for each non-matching character when repeat is enabled
- Uses the same inverted condition as `out_grouping`: `!(ch > max || (ch -= min) < 0 || (s[ch >> 3] & (0X1 << (ch & 0X7))) == 0)`
- Essential for backward pattern matching in suffix analysis, particularly for identifying vowel/consonant boundaries when processing word endings
- Commonly used in stemming rules that need to move backward from suffixes to find appropriate stem boundaries