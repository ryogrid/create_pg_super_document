# out_grouping

## Location
[src/backend/snowball/libstemmer/utilities.c:191-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L191-L202)

## Overview
The `out_grouping` function checks if characters in a Snowball stemmer environment fall outside a specified character grouping for non-UTF8 encodings.

## Definition
```c
extern int out_grouping(struct SN_env * z, const unsigned char * s, int min, int max, int repeat)
```

## Detailed Description
This function is the logical inverse of `in_grouping`, part of the Snowball stemming library utilities for character groupings in non-UTF8 cases. It examines characters at the current position in the string buffer and determines if they fall outside a specified character group defined by a bitmask. The function can operate in two modes: single character check or repeated character checking until a matching character (one that is in the grouping) is found.

The function uses the same bitmask approach as `in_grouping` but with inverted logic - it returns 1 when a character IS in the grouping (indicating failure to find a character outside the group) and continues when characters are NOT in the grouping.

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
  - [r_mark_regions](../r/r_mark_regions.md) (extensively across all language stemmers)
  - [porter_ISO_8859_1_stem](../p/porter_ISO_8859_1_stem.md) (in Porter stemmer)
  - [indonesian_ISO_8859_1_stem](../i/indonesian_ISO_8859_1_stem.md) (in Indonesian stemmer)
  - [r_KER](../r/r_KER.md) (in Indonesian stemmer)

## Notes and Other Information
- Returns -1 if at end of string buffer (z->c >= z->l)
- Returns 1 if character is found within the specified grouping (opposite of `in_grouping`)
- Returns 0 if character(s) successfully stay outside the grouping
- Uses negated condition: `!(ch > max || (ch -= min) < 0 || (s[ch >> 3] & (0X1 << (ch & 0X7))) == 0)`
- Character position (z->c) is incremented for each non-matching character when repeat is enabled
- Commonly used for skipping over consonants to find vowels, or vice versa, in region marking algorithms
- Essential for identifying syllable and morphological boundaries in various languages

## Simplified Source

```c
extern int out_grouping(struct SN_env * z, const unsigned char * s, int min, int max, int repeat) {
    do {
        int ch;

        // Check if we've reached the end of string
        if (z->c >= z->l) return -1;

        // Get character at current position
        ch = z->p[z->c];

        // Check if character is IN the grouping (opposite of in_grouping)
        if (!(ch > max || (ch -= min) < 0 || (s[ch >> 3] & (0X1 << (ch & 0X7))) == 0))
            return 1;  // Character is in grouping (fail condition)

        // Move forward in string
        z->c++;
    } while (repeat);  // Continue if repeat is enabled

    return 0;  // All characters were outside grouping
}
```