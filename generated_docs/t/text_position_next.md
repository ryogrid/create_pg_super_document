# text_position_next

## Location
src/backend/utils/adt/varlena.c: 1336 - 1399

## Overview
The  function advances the search to find the next occurrence of a pattern, handling multibyte character boundaries correctly.

## Definition

```c
static bool
text_position_next(TextPositionState *state)
```
## Detailed Description
The  function is the core iteration component of PostgreSQL's substring search system. It searches for the next occurrence of a pattern starting from the end of the previous match (or from the beginning on the first call). The function uses the Boyer-Moore-Horspool algorithm through  for efficient searching, then performs additional validation for multibyte encodings to ensure matches occur at proper character boundaries. For complex multibyte encodings, it walks character by character to verify that byte sequence matches don't occur in the middle of multibyte characters, retrying the search from the next character boundary if a false positive is detected.

## Parameters / Member Variables
- : Pointer to TextPositionState structure containing:
  - Search parameters (strings, lengths, positions)
  - Boyer-Moore-Horspool skip table for optimization
  - Reference position tracking for multibyte validation
  - Last match position for continuation

## Dependencies
- Functions called/Symbols referenced:
  -  - Performs the core Boyer-Moore-Horspool search
  -  - Gets the byte length of a multibyte character
- Called from (representative examples):
  -  - Single occurrence search
  -  - Text replacement operations (multiple occurrences)
  -  - String splitting functions
  -  - Text splitting operations

## Notes and Other Information
- Returns  if a match is found,  if no more matches exist
- Refuses to match empty-string patterns (returns false immediately)
- Starts search from the end of the previous match to avoid overlapping matches
- For example, searching for "xx" in "xxx" returns only one match, not two
- Implements character boundary validation for complex multibyte encodings
- Uses goto retry mechanism for false positive handling in multibyte scenarios
- Maintains reference position tracking to efficiently validate multibyte boundaries
- Critical component for all PostgreSQL string search operations requiring multiple matches