# r_adjectival

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_russian.c:431-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_russian.c#L431-L466)

## Overview
A composite function in PostgreSQL's Russian Snowball stemmer that handles comprehensive adjectival suffix removal, including both basic adjective endings and additional participial or compound adjectival forms.

## Definition
```c
static int r_adjectival(struct SN_env * z)
```

## Detailed Description
The r_adjectival function is a higher-level orchestrator in the Russian stemming process that handles complex adjectival morphology. It implements a two-stage approach to adjectival suffix removal:

1. **Primary Stage**: Calls r_adjective to remove standard adjectival endings (26 patterns from array a_1)
2. **Secondary Stage**: Optionally processes additional participial or compound forms by:
   - Checking for additional suffix patterns (8 patterns from array a_2)
   - Handling special cases that require prefix validation (checking for characters 0xC1 or 0xD1)
   - Managing complex morphological combinations where adjectives may have multiple layered suffixes

The function uses sophisticated backtracking logic to handle cases where the secondary stage fails - it preserves the cursor position and gracefully falls back to just the primary adjective removal. This approach ensures robust handling of Russian's complex adjectival system, including participial adjectives, compound forms, and various inflectional combinations.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the Russian word being processed
  - `z->c`: Current cursor position (moves backwards during processing)  
  - `z->ket`: End position of substring being considered for removal
  - `z->bra`: Start position of substring being considered for removal
  - `z->lb`: Left boundary of the string
  - `z->p`: Pointer to the word string
  - `z->l`: Length of the word

## Dependencies
- Functions called/Symbols referenced:
  - [r_adjective](r_adjective.md) (processes standard adjectival suffixes first)
  - [find_among_b](../f/find_among_b.md) (matches patterns from suffix array a_2)
  - [slice_del](../s/slice_del.md) (removes identified suffixes)
  - a_2 (array containing 8 additional adjectival/participial patterns)
- Called from (representative examples):
  - [russian_KOI8_R_stem](russian_KOI8_R_stem.md) (main Russian stemming function for KOI8-R encoding)
  - [russian_UTF_8_stem](russian_UTF_8_stem.md) (main Russian stemming function for UTF-8 encoding)

## Notes and Other Information
- This function exemplifies the hierarchical approach needed for morphologically rich languages like Russian
- The two-stage processing handles the fact that Russian adjectives can have multiple layers of suffixes
- Character codes 0xC1 and 0xD1 represent specific Cyrillic characters that affect participial formation
- The backtracking mechanism (using m1 and position restoration) ensures robust processing of edge cases
- Bit manipulation for character range checking (671113216 >> ...) optimizes performance for Cyrillic text
- Essential for handling Russian participial adjectives, which combine verbal and adjectival characteristics
- Always returns 1 on completion, ensuring the stemming pipeline continues even if no additional suffixes are found
- Part of the comprehensive Russian morphological analysis that includes noun, verb, and adjectival processing