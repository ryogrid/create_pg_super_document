# out_grouping_U

## Location
[src/backend/snowball/libstemmer/utilities.c:141-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L141-L152)

## Overview
Advances the cursor forward while characters do NOT belong to a specified character group, stopping when a character in the group is found.

## Definition
extern int out_grouping_U(struct SN_env * z, const unsigned char * s, int min, int max, int repeat)

## Detailed Description
This function is the logical inverse of in_grouping_U, designed to skip over characters that are NOT members of a specified character group. It continues advancing the cursor forward until it encounters a character that IS in the group, or until it reaches the end of the text. This functionality is essential for finding the boundaries between different character classes in stemming algorithms.

The function works by:
1. Decoding the UTF-8 character at the current cursor position
2. Checking if the character falls within the specified min-max range and testing group membership
3. If the character is NOT in the group, advancing the cursor and continuing
4. If the character IS in the group, stopping and returning the character's byte width
5. Optionally repeating the process to skip over consecutive non-group characters

This is particularly useful for operations like finding the end of consonant clusters, skipping non-vowels until a vowel is found, or locating specific morphological boundaries in words.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the text and cursor state
- : Bitmap representing the character group (each bit indicates group membership)
- : Minimum character value in the group range
- : Maximum character value in the group range
- : Flag indicating whether to continue skipping consecutive non-group characters

## Dependencies
- Functions called/Symbols referenced:
  - [get_utf8](../g/get_utf8.md) (for UTF-8 character decoding)
  - [repeat](../r/repeat.md) (parameter used in control flow)
- Called from (representative examples):
  - [r_mark_regions](../r/r_mark_regions.md) (in various language stemmers)
  - [porter_UTF_8_stem](../p/porter_UTF_8_stem.md)
  - [lithuanian_UTF_8_stem](../l/lithuanian_UTF_8_stem.md)
  - [indonesian_UTF_8_stem](../i/indonesian_UTF_8_stem.md)

## Notes and Other Information
- Returns 0 on successful completion when repeat is true and all characters processed are outside the group
- Returns the byte width of the first character found that IS in the group
- Returns -1 if UTF-8 decoding fails
- Uses inverted logic compared to in_grouping_U: continues while characters are NOT in the group
- The bitmap testing uses negated logic: if (!(condition)) to continue skipping
- Essential for region marking in stemming algorithms where boundaries between vowels and consonants need to be identified
- Used extensively in morphological analysis to locate transitions between different character classes
- The function is declared as extern, making it available to generated stemmer code
- Cursor movement is forward (z->c += w) and stops when a target character is found

## Simplified Source

```c
extern int out_grouping_U(struct SN_env * z, const unsigned char * s, int min, int max, int repeat) {
    do {
        int ch;
        // Decode UTF-8 character at current position
        int width = get_utf8(z->p, z->c, z->l, &ch);
        if (!width) return -1;  // UTF-8 decode error

        // Check if character IS in the group (opposite of in_grouping_U)
        if (!(ch > max || (ch -= min) < 0 || (s[ch >> 3] & (0X1 << (ch & 0X7))) == 0))
            return width;  // Character found in group, stop here

        // Character not in group, advance cursor and continue
        z->c += width;
    } while (repeat);

    return 0;  // Success - skipped all non-group characters
}
```