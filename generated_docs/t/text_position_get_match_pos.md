# text_position_get_match_pos

## Location
[src/backend/utils/adt/varlena.c:1479-1494](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1479-L1494)

## Overview
Returns the character-based position (1-based offset) of the current match found in a text search operation.

## Definition

```c
static int
text_position_get_match_pos(TextPositionState *state)
```
## Detailed Description
This function converts the byte-based position of a match to a character-based position and returns it as a 1-based offset. The function performs multibyte character length calculation to accurately determine the character position, which is essential for proper Unicode and multibyte character set support. It updates the internal reference point tracking within the TextPositionState to optimize subsequent position calculations by avoiding recalculation from the beginning of the string.

## Parameters / Member Variables
- `*state`: Pointer to a TextPositionState structure containing the search state, match information, and position tracking data
## Dependencies
- Functions called/Symbols referenced:
  - [TextPositionState](../T/TextPositionState.md) (structure accessed and modified)
  - [pg_mbstrlen_with_len](../p/pg_mbstrlen_with_len.md) (multibyte string length calculation)
- Called from (representative examples):
  - [text_position](text_position.md) (main text position function)

## Notes and Other Information
- This is a static function, accessible only within varlena.c
- Returns a 1-based character position (not 0-based)
- Handles multibyte characters correctly using pg_mbstrlen_with_len
- Updates the state's refpoint and refpos fields to cache position information for efficiency
- The function assumes that last_match has been set by a previous search operation
- Used primarily by PostgreSQL's text position functions for finding substring locations in text data

## Simplified Source
```c
static int
text_position_get_match_pos(TextPositionState *state)
{
    // Calculate character count from reference point to current match
    int char_count = pg_mbstrlen_with_len(state->refpoint,
                                         state->last_match - state->refpoint);

    // Update cumulative character position
    state->refpos += char_count;

    // Update reference point to current match for efficiency
    state->refpoint = state->last_match;

    // Return 1-based character position
    return state->refpos + 1;
}
```