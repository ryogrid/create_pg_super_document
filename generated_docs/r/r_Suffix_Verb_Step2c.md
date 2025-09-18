# r_Suffix_Verb_Step2c

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_arabic.c:1379-1402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_arabic.c#L1379-L1402)

## Overview
This function performs Step 2c of verb suffix removal in the Arabic stemming algorithm, targeting specific Arabic verb suffixes that end with character 136 and applying two different length requirements.

## Definition
```c
static int r_Suffix_Verb_Step2c(struct SN_env * z)
```

## Detailed Description
The function implements Step 2c of the verb suffix removal process in Arabic stemming. It operates by:
1. Setting the current cursor position as the end boundary (ket)
2. Performing a preliminary character check to ensure the string ends with character 136 (specific Arabic character)
3. Using pattern matching with predefined suffix array a_20 (containing 2 different verb suffixes)
4. Applying different minimum length requirements based on the matched suffix category:
   - Category 1: Requires minimum 4 UTF-8 characters after removal
   - Category 2: Requires minimum 6 UTF-8 characters after removal
5. Deleting the identified suffix if length requirements are met

This step uses character-specific filtering like Step2b but with a different target character and different length requirements.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment with:
  - `c`: Current cursor position in the string
  - `ket`: End boundary marker for suffix identification
  - `bra`: Start boundary marker for suffix identification
  - `lb`: Left boundary limit for processing
  - `p`: Pointer to the string being processed
- `among_var`: Local variable storing the category of matched suffix (1 or 2)

## Dependencies
- Functions called/Symbols referenced:
  - [len_utf8](../l/len_utf8.md): Used to check minimum word length requirements for each suffix category
  - [find_among_b](../f/find_among_b.md): Used for backward suffix pattern matching with array a_20
  - [slice_del](../s/slice_del.md): Used to delete the identified suffix from the string
- Called from (representative examples):
  - [arabic_UTF_8_stem](../a/arabic_UTF_8_stem.md): Main Arabic stemming function that orchestrates the stemming process

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit
- Uses array a_20 containing 2 different Arabic verb suffixes for pattern matching
- Implements a pre-filtering mechanism by checking for specific ending character (136)
- Uses a two-tier length threshold approach (4 and 6 characters)
- The character check ensures that only words ending with the specific Arabic character 136 are processed
- Part of the sequential verb stemming process alongside Step2a and Step2b
- Final step in the Step2 series of verb suffix removal functions
- Returns 1 on successful suffix removal and 0 when conditions are not met or no applicable suffix is found