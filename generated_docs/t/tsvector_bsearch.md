# tsvector_bsearch

## Location
[src/backend/utils/adt/tsvector_op.c:400-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L400-L432)

## Overview
A static function that performs binary search to locate a specific lexeme within a TSVector's sorted entry array.

## Definition

```c
static int
tsvector_bsearch(const TSVector tsv, char *lexeme, int lexeme_len)
```
## Detailed Description
The  function implements a classic binary search algorithm to efficiently locate a lexeme within a TSVector's word entry array. Since TSVector maintains its lexemes in alphabetical order, binary search provides O(log n) lookup performance. The function compares the search lexeme against entries in the middle of the current search range, progressively narrowing the range until the lexeme is found or determined to be absent.

The search process:
1. Maintains low and high boundary indices for the search range
2. Calculates the middle position and retrieves the corresponding lexeme
3. Uses  to compare the search lexeme with the middle entry
4. Adjusts search boundaries based on comparison result
5. Returns the index if found, or -1 if not present

## Parameters / Member Variables
- : The TSVector to search within (read-only)
- : The lexeme string to search for
- : Length of the lexeme string

## Dependencies
- Functions called/Symbols referenced:
  - ARRPTR (macro to get WordEntry array pointer)
  - STRPTR (macro to get string data pointer)
  - [tsCompareString](tsCompareString.md) (string comparison function for TSVector lexemes)
  - [WordEntry](../W/WordEntry.md) (structure type for TSVector entries)
- Called from:
  - TSVectorStat (for statistical operations)
  - [tsvector_setweight_by_filter](tsvector_setweight_by_filter.md) (for weight modification operations)
  - [tsvector_delete_str](tsvector_delete_str.md) (for single lexeme deletion)
  - [tsvector_delete_arr](tsvector_delete_arr.md) (for multiple lexeme deletion)

## Notes and Other Information
- Returns the array index (0-based) if the lexeme is found, -1 if not found
- Assumes the TSVector's lexemes are properly sorted (which is guaranteed by TSVector construction)
- Uses efficient binary search providing O(log n) time complexity
- Critical for various TSVector operations that need to locate specific lexemes
- The comparison is performed using  which handles TSVector-specific string comparison rules