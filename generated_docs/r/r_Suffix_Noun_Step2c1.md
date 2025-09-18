# r_Suffix_Noun_Step2c1

## Location
src/backend/snowball/libstemmer/stem_UTF_8_arabic.c: 1267 - 1278

## Overview
Performs Step 2c1 of Arabic noun suffix removal as part of the Arabic stemming algorithm in PostgreSQL Snowball stemmer.

## Definition
```c
static int r_Suffix_Noun_Step2c1(struct SN_env * z)
```

## Detailed Description
This function implements Step 2c1 of the Arabic noun suffix removal process in the Snowball stemming algorithm. It specifically targets a 2-byte Arabic suffix pattern ending with ta marbuta. The function operates by:

1. Setting the ket position to mark the end of the potential suffix
2. Checking for a specific byte pattern (170, which is 0xAA) at the current position
3. Using `find_among_b()` to match against the a_14 array containing the Arabic suffix:
   - ت (ta) - UTF-8 bytes 0xD8, 0xAA
4. Setting the bra position to mark the start of the matched suffix
5. Ensuring the remaining word length is at least 4 UTF-8 characters
6. Removing the matched suffix using `slice_del()`

This step targets the Arabic letter "ت" (ta), which is a common suffix in Arabic nouns and verbs.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing:
  - `ket`: End position marker for the suffix match
  - `bra`: Start position marker for the suffix match  
  - `c`: Current cursor position
  - `lb`: Left boundary position
  - `p`: Pointer to the string being processed

## Dependencies
- Functions called/Symbols referenced:
  - `[find_among_b](../f/find_among_b.md)` (Snowball backward pattern matching function)
  - `[len_utf8](../l/len_utf8.md)` (UTF-8 string length calculation)
  - `[slice_del](../s/slice_del.md)` (Snowball suffix deletion function)
- Called from (representative examples):
  - `[arabic_UTF_8_stem](../a/arabic_UTF_8_stem.md)` (main Arabic stemming function, called at lines 1522, 1557)

## Notes and Other Information
- This function specifically matches the Arabic suffix ت (ta), a common suffix in Arabic morphology
- Returns 1 on successful suffix removal, 0 if no match or constraints violated
- Part of the generated Snowball stemmer code for Arabic language processing
- The byte value 170 (0xAA) corresponds to the final byte of the ta character in UTF-8
- Maintains minimum word length of 4 characters after suffix removal
- The boundary check (c - 1 <= lb) ensures at least one character exists for the 2-byte suffix
- This step is applied multiple times in the stemming process at different stages
- [Step](../S/Step.md) 2c1 is part of a series of Step 2c operations that handle different Arabic suffix patterns