# r_perfective_gerund

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_russian.c:392-420](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_russian.c#L392-L420)

## Overview
A specialized function in PostgreSQL's Russian Snowball stemmer that identifies and removes perfective gerund endings from Russian words, which are verbal forms indicating completed actions.

## Definition
```c
static int r_perfective_gerund(struct SN_env * z)
```

## Detailed Description
The r_perfective_gerund function is part of the Russian language stemming algorithm in PostgreSQL's Snowball library. It specifically handles perfective gerund forms, which are Russian verbal forms that express completed actions and typically end in suffixes like -в, -вши, -ши.

The function implements a sophisticated pattern matching process:
1. First performs a character range check to optimize performance (checking if the character is in the expected range)
2. Uses the find_among_b function with pattern array a_0 containing 9 different perfective gerund patterns
3. Handles two different cases based on the matched pattern:
   - Case 1: Patterns requiring additional prefix checks (looking for characters 0xC1 or 0xD1 before deletion)
   - Case 2: Simple patterns that can be deleted directly
4. Removes the identified suffix using slice_del function

This function is critical for accurate Russian text processing as perfective gerunds are common verbal forms that need to be stemmed to their root form for proper indexing and searching.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the Russian word being processed
  - `z->c`: Current cursor position (moves backwards from end)
  - `z->ket`: End position of the substring being considered
  - `z->bra`: Start position of the substring being considered
  - `z->lb`: Left boundary of the string
  - `z->p`: Pointer to the word string
  - `z->l`: Length of the word

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (finds matching patterns from suffix list a_0)
  - [slice_del](../s/slice_del.md) (removes the identified suffix)
  - a_0 (array containing 9 perfective gerund patterns)
- Called from (representative examples):
  - [russian_KOI8_R_stem](russian_KOI8_R_stem.md) (main Russian stemming function for KOI8-R encoding)
  - [russian_UTF_8_stem](russian_UTF_8_stem.md) (main Russian stemming function for UTF-8 encoding)

## Notes and Other Information
- This function is specific to Russian morphology and handles the complex inflectional system of Russian perfective gerunds
- The character codes 0xC1 and 0xD1 correspond to specific Cyrillic characters that affect gerund formation
- The bit manipulation (>> 5, & 0x1f, 25166336 >> ...) is an optimized way to check character ranges for Cyrillic script
- Returns 1 on successful suffix removal, 0 if no pattern matches, and negative values for errors
- Part of the multi-step Russian stemming process that must handle prefixes, suffixes, and derivational morphology
- The function processes text backwards from the end (suffix-stripping approach)