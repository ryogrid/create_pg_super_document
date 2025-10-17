# text_position

## Location
[src/backend/utils/adt/varlena.c:1176-1215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1176-L1215)

## Overview
The  function performs the core substring search functionality, implementing the actual algorithm to find the position of a pattern within a text string.

## Definition

```c
static int
text_position(text *t1, text *t2, Oid collid)
```
## Detailed Description
The  function is the internal implementation that performs the actual substring search work for PostgreSQL's text position functions. It takes a haystack string (t1), a needle pattern (t2), and a collation ID, then returns the 1-based character position of the first occurrence of the pattern within the string. The function handles special cases like empty patterns (which always match at position 1) and cases where the haystack is shorter than the needle (which cannot match). It uses a state-based approach with setup, iteration, and cleanup phases to efficiently perform the search while respecting collation rules.

## Parameters / Member Variables
- `*t1`: The text string to be searched (haystack)
- `*t2`: The pattern to match within t1 (needle)
- `collid`: The collation ID to use for text comparison operations
## Dependencies
- Functions called/Symbols referenced:
  -  - Initializes the search state
  -  - Advances to the next potential match
  -  - Retrieves the match position from state
  -  - Cleans up the search state
  -  - Gets the size of variable-length data excluding header
- Called from (representative examples):
  -  - SQL POSITION() function wrapper
  -  - [Variable](../V/Variable.md) string processing

## Notes and Other Information
- Returns 1-based position consistent with SQL standard (not 0-based like C)
- Empty needle (pattern) always matches at position 1 by SQL standard
- Returns 0 when no match is found
- Uses efficient state-based search algorithm to handle complex collation rules
- Part of PostgreSQL's variable-length string processing infrastructure
- Designed to be called directly by other string processing functions beyond just textpos()

## Simplified Source
```c
static int
text_position(text *haystack, text *needle, Oid collation_id)
{
    TextPositionState search_state;
    int result;

    // Special case: empty needle always matches at position 1 (SQL standard)
    if (VARSIZE_ANY_EXHDR(needle) < 1) {
        return 1;
    }

    // Early exit: haystack shorter than needle cannot match
    if (VARSIZE_ANY_EXHDR(haystack) < VARSIZE_ANY_EXHDR(needle)) {
        return 0;
    }

    // Set up search state with haystack, needle, and collation
    text_position_setup(haystack, needle, collation_id, &search_state);

    // Perform the search
    if (text_position_next(&search_state)) {
        // Found a match - get the position
        result = text_position_get_match_pos(&search_state);
    } else {
        // No match found
        result = 0;
    }

    // Clean up search state
    text_position_cleanup(&search_state);

    return result;  // 1-based position, or 0 if not found
}
```