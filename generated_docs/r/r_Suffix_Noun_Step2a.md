# r_Suffix_Noun_Step2a

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_arabic.c:1244-1254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_arabic.c#L1244-L1254)

## Overview
Performs Step 2a of Arabic noun suffix removal as part of the Arabic stemming algorithm in PostgreSQL Snowball stemmer.

## Definition
```c
static int r_Suffix_Noun_Step2a(struct SN_env * z)
```

## Detailed Description
This function implements Step 2a of the Arabic noun suffix removal process in the Snowball stemming algorithm. It attempts to match and remove one of three specific Arabic suffix patterns from the end of words. The function operates by:

1. Setting the ket position to mark the end of the potential suffix
2. Using `find_among_b()` to search backwards for any of the three Arabic suffixes in the a_12 array:
   - و (waw) - UTF-8 bytes 0xD9, 0x88
   - ي (ya) - UTF-8 bytes 0xD9, 0x8A  
   - ا (alef) - UTF-8 bytes 0xD8, 0xA7
3. Setting the bra position to mark the start of the matched suffix
4. Ensuring the remaining word length is greater than 4 UTF-8 characters
5. Removing the matched suffix using `slice_del()`

This step targets common Arabic noun endings that need to be stripped to find the word root.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure containing:
  - `ket`: End position marker for the suffix match
  - `bra`: Start position marker for the suffix match  
  - `c`: Current cursor position
  - `p`: Pointer to the string being processed

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (Snowball backward pattern matching function)
  - [len_utf8](../l/len_utf8.md) (UTF-8 string length calculation)
  - [slice_del](../s/slice_del.md) (Snowball suffix deletion function)
- Called from (representative examples):
  - [arabic_UTF_8_stem](../a/arabic_UTF_8_stem.md) (main Arabic stemming function, called at lines 1508, 1543, 1570)

## Notes and Other Information
- This function matches three common Arabic suffixes: و, ي, and ا
- Returns 1 on successful suffix removal, 0 if no match or length constraint violated
- Part of the generated Snowball stemmer code for Arabic language processing
- Maintains minimum word length of 4 characters after suffix removal to prevent over-stemming
- The a_12 array contains 3 entries, each representing one of the target suffixes
- This step is applied multiple times in the stemming process at different stages

## Simplified Source

```c
static int r_Suffix_Noun_Step2a(struct SN_env * z) {
    // Find Arabic suffix pattern (و, ي, or ا)
    z->ket = z->c;
    if (!find_among_b(z, a_12, 3)) return 0;  // 3 common suffixes
    z->bra = z->c;

    // Remove suffix if minimum length > 4
    if (len_utf8(z->p) > 4) {
        slice_del(z);
        return 1;
    }

    return 0;
}
```