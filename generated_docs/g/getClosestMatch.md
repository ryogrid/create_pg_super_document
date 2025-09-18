# getClosestMatch

## Location
src/backend/utils/adt/varlena.c: 6243 - 6255

## Overview
Returns the closest matching string found during a fuzzy string matching session managed by a `ClosestMatchState` structure.

## Definition
```c
const char *getClosestMatch(ClosestMatchState *state)
```

## Detailed Description
This is a simple accessor function that retrieves the best matching candidate string that was found during a series of calls to `updateClosestMatch()`. The function performs minimal validation and simply returns the `match` field from the provided state structure.

If no suitable candidates were provided via `updateClosestMatch()`, or if none of the candidates met the distance criteria, the function returns NULL. The returned string is a pointer to the original candidate string that was passed to `updateClosestMatch()`, so the caller must ensure that string remains valid.

## Parameters / Member Variables
- `state`: Pointer to a `ClosestMatchState` structure containing the search results from previous `updateClosestMatch()` calls

## Dependencies
- Functions called/Symbols referenced:
  - `ClosestMatchState` (struct type)
- Called from (representative examples):
  - `postgresql_fdw_validator` in src/backend/foreign/foreign.c:657

## Notes and Other Information
- The function returns NULL if no suitable match was found during the matching process
- The returned pointer is to the original candidate string, so the caller must ensure the string remains valid
- This function is typically used after a series of `updateClosestMatch()` calls to retrieve the final result
- Used in PostgreSQL error reporting to suggest corrections for misspelled configuration parameters or identifiers