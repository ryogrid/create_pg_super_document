# r_Suffix_Verb_Step2b

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_arabic.c:1367-1378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_arabic.c#L1367-L1378)

## Overview
This function performs Step 2b of verb suffix removal in the Arabic stemming algorithm, targeting specific Arabic verb suffixes that end with particular characters and require a minimum word length.

## Definition
```c
static int r_Suffix_Verb_Step2b(struct SN_env * z)
```

## Detailed Description
The function implements Step 2b of the verb suffix removal process in Arabic stemming. It operates by:
1. Setting the current cursor position as the end boundary (ket)
2. Performing a preliminary character check to ensure the string ends with either character 133 or 167 (specific Arabic characters)
3. Ensuring there are at least 3 characters available for processing from the left boundary
4. Using pattern matching with predefined suffix array a_19 (containing 2 different verb suffixes)
5. Applying a single minimum length requirement of 5 UTF-8 characters after removal
6. Deleting the identified suffix if all conditions are met

This step is more restrictive than Step2a, using specific character validation before attempting pattern matching.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment with:
  - `c`: Current cursor position in the string
  - `ket`: End boundary marker for suffix identification
  - `bra`: Start boundary marker for suffix identification
  - `lb`: Left boundary limit for processing
  - `p`: Pointer to the string being processed

## Dependencies
- Functions called/Symbols referenced:
  - [len_utf8](../l/len_utf8.md): Used to check minimum word length requirement (5 characters)
  - [find_among_b](../f/find_among_b.md): Used for backward suffix pattern matching with array a_19
  - [slice_del](../s/slice_del.md): Used to delete the identified suffix from the string
- Called from (representative examples):
  - [arabic_UTF_8_stem](../a/arabic_UTF_8_stem.md): Main Arabic stemming function that orchestrates the stemming process

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit
- Uses array a_19 containing only 2 different Arabic verb suffixes for pattern matching
- Implements a pre-filtering mechanism by checking for specific ending characters (133 or 167)
- Uses a single length threshold of 5 characters, simpler than the multi-tier approach in Step2a
- The character check ensures that only words ending with specific Arabic characters are processed
- Part of the sequential verb stemming process alongside Step2a and Step2c
- Returns 1 on successful suffix removal and 0 when conditions are not met or no applicable suffix is found

## Simplified Source

```c
static int r_Suffix_Verb_Step2b(struct SN_env * z) {
    // Mark current position as end boundary
    z->ket = z->c;

    // Pre-filter: Check if word ends with specific Arabic characters (133 or 167)
    // and has at least 3 characters from left boundary
    if (z->c - 3 <= z->lb ||
        (z->p[z->c - 1] != 133 && z->p[z->c - 1] != 167)) {
        return 0;  // Doesn't meet pre-conditions
    }

    // Find matching verb suffix from small predefined array (2 suffixes)
    if (!find_among_b(z, a_19, 2)) {
        return 0;  // No suffix found
    }

    // Mark start boundary for deletion
    z->bra = z->c;

    // Ensure minimum word length of 5 UTF-8 characters
    if (len_utf8(z->p) >= 5) {
        slice_del(z);  // Delete the suffix
        return 1;      // Successfully processed
    }

    return 0;  // Word too short
}
```