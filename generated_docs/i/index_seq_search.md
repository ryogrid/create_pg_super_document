# index_seq_search

## Location
[src/backend/utils/adt/formatting.c:1099-1122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L1099-L1122)

## Overview
Fast sequential search function that uses an index for data selection to optimize format parsing by quickly filtering out unwanted strings.

## Definition

```c
static const KeyWord *
index_seq_search(const char *str, const KeyWord *kw, const int *index)
```
## Detailed Description
This function performs an optimized sequential search through a keyword array using an index-based filtering mechanism. It's specifically designed for format parsing where binary search cannot be used. The function first applies a character-based filter to quickly eliminate invalid starting characters, then uses an index array to locate the appropriate starting position in the keyword array for sequential searching.

The search algorithm:
1. Applies  to check if the first character is valid
2. Uses the character as an offset into the index array to find the starting position
3. Performs sequential comparison starting from that position
4. Continues while the first character matches, stopping when a full match is found or no more candidates exist

## Parameters / Member Variables
- : Input string to search for in the keyword array
- : Array of KeyWord structures containing the searchable keywords
- : Index array that maps characters to starting positions in the keyword array for optimization

## Dependencies
- Functions called/Symbols referenced:
  - KeyWord (struct type)
  - KeyWord_INDEX_FILTER (macro for character filtering)
  - strncmp (standard C library function)
- Called from (representative examples):
  - DCH_ZONED
  - [parse_format](../p/parse_format.md)

## Notes and Other Information
- This is a static function, only accessible within formatting.c
- Designed specifically for format parsing where binary search is not applicable
- Uses character-based indexing optimization to avoid scanning the entire keyword array
- Returns NULL if no match is found or if the initial character filter fails
- The function assumes the keyword array is properly structured with matching index array