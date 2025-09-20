# r_derivational

## Location
[src/backend/snowball/libstemmer/stem_KOI8_R_russian.c:517-530](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_KOI8_R_russian.c#L517-L530)

## Overview
The r_derivational function removes derivational suffixes from Russian words, specifically handling the suffixes "ость" and "ости" in the KOI8-R encoding variant of the Snowball Russian stemmer.

## Definition

```c
}

static int r_derivational(struct SN_env * z)
```
## Detailed Description
This function implements step 4 of the Russian stemming algorithm for KOI8-R encoded text, focusing on derivational morphology. It specifically targets the removal of the derivational suffixes "ость" (ost', abstract noun suffix) and "ости" (osti, genitive/dative/locative forms).

The function includes an important morphological constraint: it only removes these suffixes if they fall within the R2 region of the word, which represents the most distant morphological boundary. This prevents over-stemming of short words where these letter sequences might be part of the root rather than true suffixes.

The character check (bytes 212/0xD4 and 216/0xD8) corresponds to the final characters 'т' and 'и' respectively in KOI8-R encoding, which are the possible endings of the targeted derivational suffixes.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing:
  - : Current cursor position in the string
  - : End position marker for substring operations
  - : Beginning position marker for substring operations  
  - : Pointer to the string being processed
  - : Left boundary limit for processing

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Performs backward matching against suffix array
  - [r_R2](r_R2.md): Tests whether the current position is within the R2 region
  - [slice_del](../s/slice_del.md): Deletes the substring between bra and ket markers
- Data structures used:
  - a_6: Array containing 2 derivational suffix patterns ("ость", "ости")
- Called from (representative examples):
  - [russian_KOI8_R_stem](russian_KOI8_R_stem.md): Main stemming function for KOI8-R
  - [russian_UTF_8_stem](russian_UTF_8_stem.md): UTF-8 variant of the Russian stemmer

## Notes and Other Information
- This function handles a critical step in Russian morphological analysis by removing common abstract noun suffixes
- The R2 constraint (via r_R2 function call) ensures morphologically sound stemming by preventing removal from word roots
- Processes only 2 specific patterns but these are very common in Russian derivational morphology  
- Character codes 212 (0xD4) and 216 (0xD8) correspond to 'т' and 'и' in KOI8-R encoding
- Returns 1 on successful suffix removal, 0 if no pattern matched or R2 constraint failed
- Part of the automatically generated Snowball stemmer code
- This step typically occurs later in the stemming pipeline after inflectional suffixes have been processed
- The derivational suffixes "ость"/"ости" are highly productive in Russian for creating abstract nouns from adjectives