# r_Step_1c

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c:612-635](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_english.c#L612-L635)

## Overview
The r_Step_1c function implements Step 1c of the English Porter stemming algorithm, handling the transformation of 'y' or 'Y' suffixes to 'i' when preceded by a non-vowel.

## Definition
```c
static int r_Step_1c(struct SN_env * z)
```

## Detailed Description
This function performs a specific transformation rule in the English stemming algorithm:

1. **Suffix Detection**: Looks for words ending in 'y' or 'Y'
2. **Context Validation**: Ensures the 'y'/'Y' is preceded by a consonant (non-vowel)
3. **Position Check**: Verifies the consonant is not at the very beginning of the word
4. **Transformation**: Replaces the 'y'/'Y' with 'i'

This step is crucial for handling English words where 'y' functions as a vowel in certain contexts but needs to be normalized to 'i' for proper stemming (e.g., "happy" → "happi", "city" → "citi").

The function uses a branching approach to check both lowercase 'y' and uppercase 'Y', ensuring comprehensive coverage of input variations.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the stemming environment with:
  - : Current cursor position
  - : Length of the string
  - : Left boundary limit  
  - : Character array being processed
  - : End marker for current suffix
  - : Start marker for current suffix

## Dependencies
- Functions called/Symbols referenced:
  - [out_grouping_b](../o/out_grouping_b.md) (checks if character is outside vowel group)
  - [slice_from_s](../s/slice_from_s.md) (replaces marked substring with specified string)
  - g_v (vowel character group: a,e,i,o,u,y range 97-121)
  - s_8 (replacement string: "i")
- Called from (representative examples):
  - [english_ISO_8859_1_stem](../e/english_ISO_8859_1_stem.md)
  - [porter_ISO_8859_1_stem](../p/porter_ISO_8859_1_stem.md)
  - [english_UTF_8_stem](../e/english_UTF_8_stem.md)
  - [porter_UTF_8_stem](../p/porter_UTF_8_stem.md)

## Notes and Other Information
- Returns 1 on successful transformation, 0 if no changes were made, or negative values on error
- Only transforms y/Y when preceded by a consonant and not at the word beginning
- This step normalizes the ambiguous nature of 'y' in English, where it can function as both consonant and vowel
- Essential for consistent stemming results in words ending with 'y' (cities → citi, flies → fli)
- Simple but important step in the Porter algorithm that improves stemming accuracy for y-final words

## Simplified Source

```c
static int r_Step_1c(struct SN_env * z) {
    // Mark end position for suffix
    z->ket = z->c;

    // Check for 'y' or 'Y' at current position
    if (z->c <= z->lb || (z->p[z->c - 1] != 'y' && z->p[z->c - 1] != 'Y')) {
        return 0; // No y/Y suffix found
    }
    z->c--; // Move cursor back to include the y/Y

    // Mark start position for suffix
    z->bra = z->c;

    // Check if preceded by consonant (not in vowel group)
    if (out_grouping_b(z, g_v, 97, 121, 0)) return 0;

    // Ensure consonant is not at word beginning
    if (z->c <= z->lb) return 0;

    // Replace y/Y with 'i'
    return slice_from_s(z, 1, s_8);
}
```