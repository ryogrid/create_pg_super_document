# r_undouble

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_1_danish.c: 255 - 273

## Overview
This function removes doubled consonants at the end of words by detecting repeated consonant patterns and deleting the duplicate, used in multiple European language stemmers.

## Definition


## Detailed Description
The r_undouble function implements a general-purpose consonant undoubling algorithm used across multiple European language stemmers (Danish, Dutch, Hungarian). The function operates through a sophisticated pattern matching and comparison process:

1. **Boundary Validation**: Ensures the current position is within the valid processing region (beyond I[1] marker)

2. **Consonant Detection**: Uses backward grouping to identify a consonant from the character set g_c (characters 98-122, covering 'b' through 'z')

3. **Pattern Capture**: Captures the identified consonant sequence into a string buffer (S[0]) using slice_to

4. **Duplication Check**: Verifies if the captured pattern is immediately followed by an identical pattern using eq_v_b (equivalent value backward comparison)

5. **Undoubling**: If a doubled pattern is detected, removes one instance using slice_del

The function uses temporary boundary management to constrain the search within appropriate word regions and includes error handling for memory allocation failures.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the word being processed
  - : Current cursor position in the word
  - : Region boundary marker (from r_mark_regions)
  - : Left boundary limit  
  - : End position of matched substring
  - : Beginning position of matched substring
  - : String buffer for storing captured consonant pattern

## Dependencies
- Functions called/Symbols referenced:
  - [in_grouping_b](../i/in_grouping_b.md) (backward consonant group checking for g_c, characters 98-122)
  - [slice_to](../s/slice_to.md) (pattern capture into string buffer S[0])
  - [eq_v_b](../e/eq_v_b.md) (backward pattern equivalence comparison with S[0])
  - [slice_del](../s/slice_del.md) (pattern deletion operation)
  - g_c (consonant grouping definition covering 'b'-'z')
- Called from (representative examples):
  - [danish_ISO_8859_1_stem](../d/danish_ISO_8859_1_stem.md) (Danish stemming main function)
  - [danish_UTF_8_stem](../d/danish_UTF_8_stem.md) (UTF-8 Danish variant)
  - [r_e_ending](r_e_ending.md) (Dutch stemming - e-ending processing)
  - [r_en_ending](r_en_ending.md) (Dutch stemming - en-ending processing)  
  - [r_standard_suffix](r_standard_suffix.md) (Dutch stemming - standard suffix processing)
  - [r_instrum](r_instrum.md) (Hungarian stemming - instrumental case processing)
  - [r_factive](r_factive.md) (Hungarian stemming - factive case processing)

## Notes and Other Information
- This is a widely reused function across multiple language stemmers, indicating its general applicability
- The consonant range g_c (98-122) covers all lowercase Latin consonants from 'b' to 'z'
- Returns -1 on memory allocation failure (when slice_to fails), 0 if no doubled pattern found, 1 if successful
- Uses dynamic string buffer (S[0]) rather than fixed patterns, making it adaptable to various consonant combinations
- The function is particularly important for languages with frequent consonant doubling like Danish, Dutch, and Hungarian
- Pattern capture and comparison approach allows detection of any doubled consonant sequence, not just specific predefined patterns
- Boundary management ensures undoubling only occurs in morphologically appropriate word regions