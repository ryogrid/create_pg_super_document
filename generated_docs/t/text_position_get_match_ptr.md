# text_position_get_match_ptr

## Location
[src/backend/utils/adt/varlena.c:1468-1478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1468-L1478)

## Overview
Returns a pointer to the current match found in a text search operation within the TextPositionState structure.

## Definition

```c
static char *
text_position_get_match_ptr(TextPositionState *state)
```
## Detailed Description
This is a simple accessor function that retrieves the pointer to the last successful match found during text position searching operations. The function returns the  field from the TextPositionState structure, which points directly into the original haystack string where a match was found. This allows callers to access the actual matched text without needing to know the internal structure details.

## Parameters / Member Variables
- `*state`: Pointer to a TextPositionState structure containing the search state and results
## Dependencies
- Functions called/Symbols referenced:
  - [TextPositionState](../T/TextPositionState.md) (structure accessed)
- Called from (representative examples):
  - [replace_text](../r/replace_text.md)
  - [split_part](../s/split_part.md)
  - [split_text](../s/split_text.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the varlena.c file
- The returned pointer points into the original haystack string, not a copy
- The function is used as part of PostgreSQL's text manipulation functions for string replacement and splitting operations
- The pointer is only valid as long as the original haystack string remains unchanged and in scope

## Simplified Source

```c
static char *
text_position_get_match_ptr(TextPositionState *state)
{
    // Return pointer to the current match location in the original string
    return state->last_match;
}
```