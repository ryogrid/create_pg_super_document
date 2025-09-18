# r_adjective

## Location
src/backend/snowball/libstemmer/stem_UTF_8_russian.c: 421 - 430

## Overview
A function in PostgreSQL's Russian Snowball stemmer that identifies and removes Russian adjectival endings from words during the stemming process.

## Definition
```c
static int r_adjective(struct SN_env * z)
```

## Detailed Description
The r_adjective function is a specialized component of the Russian language stemming algorithm that handles the removal of adjectival suffixes. Russian adjectives have complex inflectional patterns with multiple endings that vary based on gender, number, case, and animacy.

The function operates through a streamlined process:
1. Sets the end boundary (ket) to the current cursor position
2. Performs an optimized character range check using bit manipulation to quickly verify if the preceding character falls within the expected range for Russian adjectival endings
3. Uses find_among_b with pattern array a_1 (containing 26 different adjectival patterns) to match potential suffixes
4. If a match is found, sets the start boundary (bra) and removes the suffix using slice_del

This function is crucial for Russian text processing because adjectives are highly inflected in Russian, and proper stemming requires recognizing and removing these various adjectival forms to reveal the base form needed for search and indexing operations.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the Russian word being processed
  - `z->c`: Current cursor position (processing backwards from end)
  - `z->ket`: End position of the substring being considered for removal
  - `z->bra`: Start position of the substring being considered for removal
  - `z->lb`: Left boundary of the string
  - `z->p`: Pointer to the word string

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (pattern matching function using suffix list a_1)
  - [slice_del](../s/slice_del.md) (removes the identified adjectival suffix)
  - a_1 (array containing 26 Russian adjectival patterns)
- Called from (representative examples):
  - [r_adjectival](r_adjectival.md) (higher-level function that calls this for adjectival processing)
  - [armenian_UTF_8_stem](../a/armenian_UTF_8_stem.md) (Armenian stemmer that uses similar adjective logic)

## Notes and Other Information
- The bit manipulation (z->p[z->c - 1] >> 5 != 6 and 2271009 >> ...) provides optimized character range checking for Cyrillic script
- Array a_1 contains 26 patterns, reflecting the rich morphological diversity of Russian adjectival endings
- This function is part of a multi-stage Russian stemming process and is typically called from r_adjectival
- Returns 1 on successful suffix identification and removal, 0 if no pattern matches
- The function processes text backwards (suffix-stripping approach) which is efficient for agglutinative morphology
- Critical for handling Russian's complex adjectival agreement system in search applications