# r_Suffix_Noun_Step2b

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_arabic.c:1255-1266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_arabic.c#L1255-L1266)

## Overview
Performs Step 2b of Arabic noun suffix removal as part of the Arabic stemming algorithm in PostgreSQL Snowball stemmer.

## Definition
```c
static int r_Suffix_Noun_Step2b(struct SN_env * z)
```

## Detailed Description
This function implements Step 2b of the Arabic noun suffix removal process in the Snowball stemming algorithm. It specifically targets a 4-byte Arabic suffix pattern. The function operates by:

1. Setting the ket position to mark the end of the potential suffix
2. Performing a boundary check to ensure there are at least 3 characters before the current position
3. Checking for a specific byte pattern (170, which is 0xAA) at the end of the suffix
4. Using `find_among_b()` to match against the a_13 array containing the Arabic suffix:
   - ات (alef + ta marbuta) - UTF-8 bytes 0xD8, 0xA7, 0xD8, 0xAA
5. Setting the bra position to mark the start of the matched suffix
6. Ensuring the remaining word length is at least 5 UTF-8 characters
7. Removing the matched suffix using `slice_del()`

This step targets the common Arabic feminine plural suffix "ات" which appears on many noun forms.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing:
  - `ket`: End position marker for the suffix match
  - `bra`: Start position marker for the suffix match  
  - `c`: Current cursor position
  - `lb`: Left boundary position
  - `p`: Pointer to the string being processed

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (Snowball backward pattern matching function)
  - [len_utf8](../l/len_utf8.md) (UTF-8 string length calculation)
  - [slice_del](../s/slice_del.md) (Snowball suffix deletion function)
- Called from (representative examples):
  - [arabic_UTF_8_stem](../a/arabic_UTF_8_stem.md) (main Arabic stemming function, called at lines 1515, 1550, 1577)

## Notes and Other Information
- This function specifically matches the Arabic suffix ات (alef + ta marbuta), a common feminine plural ending
- Returns 1 on successful suffix removal, 0 if no match or constraints violated
- Part of the generated Snowball stemmer code for Arabic language processing
- The byte value 170 (0xAA) corresponds to the final byte of the ta marbuta character in UTF-8
- Maintains minimum word length of 5 characters after suffix removal
- The boundary check (c - 3 <= lb) ensures sufficient characters exist for the 4-byte suffix
- This step is applied multiple times in the stemming process at different stages

## Simplified Source

```c
static int r_Suffix_Noun_Step2b(struct SN_env * z) {
    // Check for Arabic character (170) and find ات suffix pattern
    z->ket = z->c;
    if (z->c - 3 <= z->lb || z->p[z->c - 1] != 170) return 0;

    if (!find_among_b(z, a_13, 1)) return 0;  // Find ات (feminine plural)
    z->bra = z->c;

    // Remove suffix if minimum length >= 5
    if (len_utf8(z->p) >= 5) {
        slice_del(z);
        return 1;
    }

    return 0;
}
```