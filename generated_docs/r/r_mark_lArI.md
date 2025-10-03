# r_mark_lArI

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:666-671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L666-L671)

## Overview
A static function in the Turkish stemmer that identifies and marks the specific Turkish suffix pattern 'lArI' and its vowel harmony variants.

## Definition

```c
}

static int r_mark_lArI(struct SN_env * z)
```
## Detailed Description
This function is part of the Turkish language stemming implementation that specifically handles the 'lArI' suffix pattern, which is a common Turkish plural possessive suffix. The function first performs a boundary check to ensure there are at least 3 characters available for processing. It then checks if the last character is either 'ı' (105) or 'ı' (177) in UTF-8 encoding, which are the expected final characters for this suffix pattern. Finally, it uses  to match against an array of 2 specific 'lArI' patterns () that account for Turkish vowel harmony variations.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the word being processed, current position markers, and other stemming state information
## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (Snowball library function for backwards pattern matching)
  - a_1 (array of 2 'lArI' suffix patterns with vowel harmony variants)
- Called from (representative examples):
  - [r_stem_suffix_chain_before_ki](r_stem_suffix_chain_before_ki.md)
  - [r_stem_noun_suffixes](r_stem_noun_suffixes.md)

## Notes and Other Information
- Returns 1 on successful 'lArI' suffix identification, 0 on failure
- Requires minimum 3 characters from the left boundary (z->c - 3 <= z->lb)
- Character codes 105 and 177 correspond to different variants of 'ı' in UTF-8 encoding
- The 'lArI' suffix is important in Turkish grammar as it indicates plural possession
- Part of the comprehensive suffix identification system for Turkish morphological analysis
- The function is highly specific to this particular suffix pattern, making it efficient for targeted matching