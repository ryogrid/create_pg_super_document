# r_Suffix_Noun_Step3

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_arabic.c:1291-1302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_arabic.c#L1291-L1302)

## Overview
This function performs Step 3 of noun suffix removal in the Arabic stemming algorithm, specifically targeting certain Arabic noun suffixes that need to be removed during the text normalization process.

## Definition
```c
static int r_Suffix_Noun_Step3(struct SN_env * z)
```

## Detailed Description
The function implements a specific step in the Arabic stemming algorithm that removes certain noun suffixes. It operates by:
1. Setting the current cursor position as the end boundary (ket)
2. Checking if the character before the current position matches a specific Arabic character (138)
3. Using pattern matching with a predefined suffix array (a_16) to identify valid suffixes
4. Ensuring the remaining word length would be at least 3 UTF-8 characters after removal
5. Deleting the identified suffix if all conditions are met

The function follows the standard Snowball stemmer pattern where it returns 1 on successful suffix removal and 0 when no applicable suffix is found.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment with:
  - `c`: Current cursor position in the string
  - `ket`: End boundary marker for suffix identification
  - `bra`: Start boundary marker for suffix identification
  - `lb`: Left boundary limit for processing
  - `p`: Pointer to the string being processed

## Dependencies
- Functions called/Symbols referenced:
  - [len_utf8](../l/len_utf8.md): Used to check minimum word length requirements
  - [find_among_b](../f/find_among_b.md): Used for backward suffix pattern matching with array a_16
  - [slice_del](../s/slice_del.md): Used to delete the identified suffix from the string
- Called from (representative examples):
  - [arabic_UTF_8_stem](../a/arabic_UTF_8_stem.md): Main Arabic stemming function that orchestrates the stemming process

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit
- The function specifically checks for Arabic character 138 (likely a specific Arabic suffix character)
- Uses array a_16 for pattern matching, which contains the specific noun suffixes handled in this step
- Ensures word integrity by maintaining a minimum length of 3 UTF-8 characters after suffix removal
- Part of the larger Arabic stemming algorithm implemented in the Snowball stemming framework