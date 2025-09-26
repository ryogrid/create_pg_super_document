# seq_search_ascii

## Location
[src/backend/utils/adt/formatting.c:2578-2634](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L2578-L2634)

## Overview
A static function that performs a case-insensitive sequential search through a null-terminated array of strings to find a match with the initial characters of a given input string, using ASCII-only case conversion rules.

## Definition

```c
static int
seq_search_ascii(const char *name, const char *const *array, int *len)
```
## Detailed Description
This function searches through an array of null-terminated strings for a case-insensitive match to the beginning of the input string . The function is optimized by handling the first character comparison separately to improve performance. It uses  for case conversion, making it suitable only for ASCII strings. The function returns the array index of the first match found, or -1 if no match is found. It also sets the output parameter  to indicate how many characters from the input string were matched.

The search algorithm:
1. Converts the first character of the input to lowercase using ASCII rules
2. Iterates through each string in the array
3. Compares first characters (case-insensitive)
4. If first characters match, compares the rest of the string character by character
5. Returns immediately upon finding a complete match of any array element

## Parameters / Member Variables
- : Input string to search for matches against
- : Null-terminated array of string pointers to search through
- : Output parameter - set to the length of the matched portion, or 0 for no match

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ascii_tolower](../p/pg_ascii_tolower.md) (called 4 times for case-insensitive ASCII comparisons)
- Called from (representative examples):
  - DCH_ZONED (formatting.c:1064)
  - [from_char_seq_search](../f/from_char_seq_search.md) (formatting.c:2726)

## Notes and Other Information
- This is a static function, only accessible within formatting.c
- Designed specifically for ASCII strings - does not handle Unicode case conversion
- The function is optimized for performance by checking the first character separately
- Used primarily in PostgreSQL's date/time formatting functionality
- Returns -1 and sets *len to 0 when no match is found or when input name is empty
- The matched length can be shorter than the full input string if a shorter array element matches