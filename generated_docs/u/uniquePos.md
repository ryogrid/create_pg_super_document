# uniquePos

## Location
src/backend/utils/adt/tsvector.c: 52 - 86

## Overview
Removes duplicate position entries from a WordEntryPos array while preserving the highest weight for each position.

## Definition
```c
static int uniquePos(WordEntryPos *a, int l)
```

## Detailed Description
This function processes an array of WordEntryPos structures to eliminate duplicates based on position values. Unlike a simple unique operation, it implements intelligent duplicate handling by preserving the entry with the highest weight when multiple entries share the same position. The function first sorts the array using compareWordEntryPos, then performs a single pass to remove duplicates while maintaining weight precedence. It also enforces limits on the maximum number of positions (MAXNUMPOS) and maximum entry position value (MAXENTRYPOS) to prevent overflow conditions.

## Parameters / Member Variables
- `a`: Array of WordEntryPos structures to process for duplicates
- `l`: Length of the input array (number of WordEntryPos elements)

## Dependencies
- Functions called/Symbols referenced:
  - qsort (standard library sorting function)
  - compareWordEntryPos (comparator for WordEntryPos sorting)
  - WEP_GETPOS (macro to extract position from WordEntryPos)
  - WEP_GETWEIGHT (macro to extract weight from WordEntryPos)
  - WEP_SETWEIGHT (macro to set weight in WordEntryPos)
  - MAXNUMPOS (maximum number of positions allowed)
  - MAXENTRYPOS (maximum position value allowed)
- Called from (representative examples):
  - uniqueentry (during tsvector entry processing)

## Notes and Other Information
- Returns the new length of the array after duplicate removal
- Modifies the input array in-place for memory efficiency
- Handles edge case where array length is 1 or less by returning immediately
- Implements early termination when reaching maximum position limits
- Critical for maintaining tsvector data integrity in PostgreSQL's full-text search
- The weight preservation logic ensures that more important word occurrences are retained