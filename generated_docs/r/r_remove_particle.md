# r_remove_particle

## Location
src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c: 123 - 134

## Overview
A static function in the Indonesian stemmer that removes particle suffixes from Indonesian words as part of the morphological stemming process.

## Definition
```c
static int r_remove_particle(struct SN_env * z)
```

## Detailed Description
This function is part of the Snowball stemming algorithm implementation for the Indonesian language. It performs suffix removal by:

1. Setting the 'ket' position to the current cursor position
2. Checking if the word ends with specific characters (104='h' or 110='n') 
3. Using backward pattern matching to find particle suffixes in the predefined array 'a_0'
4. Removing the matched suffix if found
5. Decrementing a counter (z->I[1]) to track the removal

The function returns 1 on successful removal, 0 if no particle suffix is found, or a negative value on error.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment with word buffer, cursor positions, and counters

## Dependencies
- Functions called/Symbols referenced:
  - find_among_b (backward pattern matching function)
  - slice_del (suffix deletion function)
  - a_0 (predefined array of particle patterns, likely containing 3 entries)
- Called from (representative examples):
  - indonesian_ISO_8859_1_stem (at src/backend/snowball/libstemmer/stem_ISO_8859_1_indonesian.c:336)
  - indonesian_UTF_8_stem (at src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:336)

## Notes and Other Information
- This is part of PostgreSQL's full-text search capabilities, specifically for Indonesian text processing
- The function checks boundary conditions (z->c - 2 <= z->lb) to ensure safe buffer access
- The counter z->I[1] appears to track the number of suffix removals performed
- The function uses character codes 104 ('h') and 110 ('n') for optimization, checking only words ending with these characters