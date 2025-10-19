# r_Suffix_Noun_Step2c2

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_arabic.c:1279-1290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_arabic.c#L1279-L1290)

## Overview
Performs Step 2c2 of Arabic noun suffix removal as part of the Arabic stemming algorithm in PostgreSQL Snowball stemmer.

## Definition
```c
static int r_Suffix_Noun_Step2c2(struct SN_env * z)
```

## Detailed Description
This function implements Step 2c2 of the Arabic noun suffix removal process in the Snowball stemming algorithm. It specifically targets a 2-byte Arabic suffix pattern ending with ta marbuta (feminine marker). The function operates by:

1. Setting the ket position to mark the end of the potential suffix
2. Checking for a specific byte pattern (169, which is 0xA9) at the current position
3. Using `find_among_b()` to match against the a_15 array containing the Arabic suffix:
   - ة (ta marbuta) - UTF-8 bytes 0xD8, 0xA9
4. Setting the bra position to mark the start of the matched suffix
5. Ensuring the remaining word length is at least 4 UTF-8 characters
6. Removing the matched suffix using `slice_del()`

This step targets the Arabic letter "ة" (ta marbuta), which is the primary feminine marker suffix in Arabic nouns and adjectives.

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
  - [arabic_UTF_8_stem](../a/arabic_UTF_8_stem.md) (main Arabic stemming function, called at line 1492)

## Notes and Other Information
- This function specifically matches the Arabic suffix ة (ta marbuta), the primary feminine marker in Arabic
- Returns 1 on successful suffix removal, 0 if no match or constraints violated
- Part of the generated Snowball stemmer code for Arabic language processing
- The byte value 169 (0xA9) corresponds to the final byte of the ta marbuta character in UTF-8
- Maintains minimum word length of 4 characters after suffix removal
- The boundary check (c - 1 <= lb) ensures at least one character exists for the 2-byte suffix
- This is the final step in the Step 2c series, focusing specifically on feminine nouns
- Ta marbuta is one of the most common suffixes in Arabic, marking feminine gender
- Used less frequently than other step 2 functions, appearing only once in the main stemming algorithm

## Simplified Source

```c
static int r_Suffix_Noun_Step2c2(struct SN_env * z) {
    // Check for Arabic character (169) and find ة suffix pattern
    z->ket = z->c;
    if (z->c - 1 <= z->lb || z->p[z->c - 1] != 169) return 0;

    if (!find_among_b(z, a_15, 1)) return 0;  // Find ة (ta marbuta - feminine)
    z->bra = z->c;

    // Remove suffix if minimum length >= 4
    if (len_utf8(z->p) >= 4) {
        slice_del(z);
        return 1;
    }

    return 0;
}
```