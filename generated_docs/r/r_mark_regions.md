# r_mark_regions

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_danish.c:136-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_danish.c#L136-L157)

## Overview
This function identifies and marks vowel/consonant regions within a word to establish boundaries for suffix removal in the Snowball stemming algorithm for Basque language processing.

## Definition

```c
struct among a_2[5] =
{
{ 2, s_2_0, -1, 1, 0},
{ 3, s_2_1, 0, 1, 0},
{ 4, s_2_2, 1, 1, 0},
{ 3, s_2_3, -1, 1, 0},
{ 4, s_2_4, -1, 2, 0}
};
```
## Detailed Description
The r_mark_regions function is a critical component of the Snowball stemming algorithm that analyzes the morphological structure of words by identifying regions based on vowel-consonant patterns. It sets three region markers (I[0], I[1], I[2]) in the SN_env structure that define boundaries where different stemming rules can be applied.

The function implements a complex state machine that:
1. Initially sets all region markers to the end of the word (z->l)
2. Searches for specific vowel-consonant patterns from the beginning of the word
3. Marks the first region (I[2]) based on the first vowel-consonant or consonant-vowel transition
4. Identifies subsequent regions (I[1] and I[0]) by finding alternating vowel-consonant sequences

The algorithm uses backtracking with labeled goto statements to handle multiple possible pattern matches, ensuring robust region identification across different word structures.

## Parameters / Member Variables
- : Pointer to SN_env structure containing the word being processed and region markers
  - : Current cursor position in the word
  - : Length of the word
  - : Third region marker (furthest from word start)
  - : Second region marker (middle region)
  - : First region marker (closest to word start)

## Dependencies
- Functions called/Symbols referenced:
  - [in_grouping](../i/in_grouping.md) (vowel group checking - lines 912, 923, 925, 945, 964, 975)
  - [out_grouping](../o/out_grouping.md) (consonant group checking - lines 914, 916, 934, 936, 938, 959, 970)
  - g_v (vowel grouping definition for characters 97-117, a-u)
- Called from (representative examples):
  - [basque_ISO_8859_1_stem](../b/basque_ISO_8859_1_stem.md)
  - [catalan_ISO_8859_1_stem](../c/catalan_ISO_8859_1_stem.md)  
  - [danish_ISO_8859_1_stem](../d/danish_ISO_8859_1_stem.md)
  - [dutch_ISO_8859_1_stem](../d/dutch_ISO_8859_1_stem.md)
  - [english_ISO_8859_1_stem](../e/english_ISO_8859_1_stem.md)
  - And many other language-specific stemming functions

## Notes and Other Information
- This function appears in multiple language stemmer implementations with identical logic, suggesting it's a core algorithm component
- The function always returns 1, indicating successful region marking
- The vowel group g_v covers ASCII characters 97-117 (a-u), which includes standard vowels
- Region markers are used by subsequent suffix removal functions to determine where stemming rules can be applied
- The complex goto-based control flow is typical of generated Snowball stemmer code