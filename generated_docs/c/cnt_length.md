# cnt_length

## Location
[src/backend/utils/adt/tsrank.c:53-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L53-L74)

## Overview
Calculates the total length (number of positions) of all lexemes in a TSVector, used for text search ranking normalization.

## Definition

```c
static int
cnt_length(TSVector t)
```
## Detailed Description
The  function computes the total number of word positions contained within a TSVector structure. It iterates through all WordEntry elements in the TSVector and sums up their position data lengths. For lexemes without position data (clen == 0), it counts each as having length 1. This total length is typically used in ranking algorithms to normalize scores based on document length.

The function traverses the WordEntry array from the beginning (ARRPTR) to the end (STRPTR), examining each entry's position data length using the POSDATALEN macro. This provides an accurate count of the total positional information available for ranking calculations.

## Parameters / Member Variables
- : The TSVector structure containing lexemes and their positional information

## Dependencies
- Functions called/Symbols referenced:
  -  (macro to get array pointer from TSVector)
  -  (macro to get string pointer from TSVector)
  -  (macro to get position data length)
  -  (structure representing a lexeme entry)
  -  (text search vector type)
- Called from (representative examples):
  -  (src/backend/utils/adt/tsrank.c:376, 380)
  -  (src/backend/utils/adt/tsrank.c:924, 928)

## Notes and Other Information
- This is a static function, accessible only within tsrank.c
- The function handles both lexemes with explicit position data (clen > 0) and those without (clen == 0)
- For lexemes without position information, the function assumes a length of 1, ensuring they still contribute to the total document length calculation
- The returned length is used in ranking normalization to prevent longer documents from having artificially higher scores
- Essential component of PostgreSQL's text search ranking system for fair comparison between documents of different lengths