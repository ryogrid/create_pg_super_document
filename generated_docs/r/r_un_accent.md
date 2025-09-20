# r_un_accent

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_french.c:1137-1163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_french.c#L1137-L1163)

## Overview
The r_un_accent function removes specific French accent marks (é and è) that follow a consonant in the French Snowball stemming algorithm, converting them to their unaccented equivalent 'e'.

## Definition

```c
}

static int r_un_accent(struct SN_env * z)
```
## Detailed Description
The r_un_accent function performs accent normalization as part of the French stemming process. It specifically targets the acute accent (é, 0xE9) and grave accent (è, 0xE8) characters that appear after consonants. The function first uses out_grouping_b to move backwards through the text until it finds a consonant (non-vowel character), then checks if the character immediately following that consonant is either é or è. If such an accented character is found, it replaces it with a plain 'e' using slice_from_s with string s_32.

This accent removal is crucial for proper French stemming because many French words have variations with and without accents that should be treated as equivalent during morphological analysis. The function ensures that words like 'créer' and 'creer' would be processed consistently.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing the word being processed, cursor positions, and stemming boundaries

## Dependencies
- Functions called/Symbols referenced:
  - [out_grouping_b](../o/out_grouping_b.md): Moves cursor backward while characters are outside the vowel group g_v
  - [slice_from_s](../s/slice_from_s.md): Replaces the selected text slice with the string s_32 (containing 'e')
  - g_v: Vowel grouping definition for French characters (range 97-251)
  - s_32: String constant containing the replacement character 'e'
- Called from (representative examples):
  - [french_ISO_8859_1_stem](../f/french_ISO_8859_1_stem.md): Main French stemming function for ISO-8859-1 encoding
  - [french_UTF_8_stem](../f/french_UTF_8_stem.md): Main French stemming function for UTF-8 encoding

## Notes and Other Information
This function operates specifically on ISO-8859-1 encoded text where é is represented as 0xE9 and è as 0xE8. The function is part of the postlude phase of French stemming and works alongside other normalization functions like r_un_double. The accent removal only occurs when the accented character follows a consonant, preserving accent patterns that are linguistically significant in other contexts.