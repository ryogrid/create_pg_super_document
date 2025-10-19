# text_position_reset

## Location
[src/backend/utils/adt/varlena.c:1495-1502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1495-L1502)

## Overview
Resets the text position search state to its initial condition, preparing for a new search from the beginning of the string.

## Definition

```c
static void
text_position_reset(TextPositionState *state)
```
## Detailed Description
This function reinitializes the TextPositionState structure to its initial state as if it were just set up by text_position_setup. It clears any previous match information and resets the position tracking to the beginning of the haystack string. After calling this function, the next call to text_position_next will start searching from the very beginning of the string, effectively allowing for a fresh search operation without needing to recreate the entire search state.

## Parameters / Member Variables
- `*state`: Pointer to a TextPositionState structure to be reset
## Dependencies
- Functions called/Symbols referenced:
  - [TextPositionState](../T/TextPositionState.md) (structure accessed and modified)
- Called from (representative examples):
  - [split_part](../s/split_part.md) (for restarting searches when processing multiple parts)

## Notes and Other Information
- This is a static function, accessible only within varlena.c
- Sets last_match to NULL, indicating no current match
- Resets refpoint to str1 (beginning of the haystack string)  
- Resets refpos to 0 (character position counter)
- Preserves the search pattern and skip table setup from the original initialization
- Useful for functions that need to perform multiple searches on the same string, such as split operations
- Does not deallocate any memory or destroy the search setup - only resets search position

## Simplified Source

This function resets the text search state to start over from the beginning of the string, clearing any previous match information.

```c
static void
text_position_reset(TextPositionState *state)
{
    // Clear previous match and reset to start of string
    state->last_match = NULL;
    state->refpoint = state->str1;  // Back to beginning of haystack
    state->refpos = 0;              // Reset character position counter
}
```