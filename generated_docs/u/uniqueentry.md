# uniqueentry

## Location
src/backend/utils/adt/tsvector.c: 103 - 174

## Overview
Sorts an array of WordEntryIN structures, removes duplicate entries, and merges their positional information while calculating required buffer space.

## Definition
```c
static int uniqueentry(WordEntryIN *a, int l, char *buf, int *outbuflen)
```

## Detailed Description
This function processes an array of WordEntryIN structures to eliminate duplicates based on string content while intelligently merging positional information from duplicate entries. It first sorts the array using compareentry, then performs a single pass to identify and merge duplicates. When duplicate entries are found (same string content), their positional information is consolidated - if both entries have positions, they are merged into a single array; if only one has positions, those positions are preserved. The function also calculates the total buffer space needed for the final data structure, including proper alignment for position data. Each entry's positions are further processed through uniquePos to remove duplicate positions within individual entries.

## Parameters / Member Variables
- `a`: Array of WordEntryIN structures to process for duplicates
- `l`: Length of the input array (number of WordEntryIN elements)
- `buf`: Buffer containing the actual string data referenced by entries
- `outbuflen`: Pointer to receive the calculated buffer space needed for output

## Dependencies
- Functions called/Symbols referenced:
  - qsort_arg (sorting function with additional argument)
  - [compareentry](../c/compareentry.md) (comparator for WordEntryIN sorting)
  - [uniquePos](uniquePos.md) (removes duplicate positions within entries)
  - strncmp (string comparison function)
  - memcpy (memory copy function)
  - [repalloc](../r/repalloc.md) (PostgreSQL memory reallocation function)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - SHORTALIGN (alignment macro)
  - WordEntryIN (input structure type)
  - WordEntryPos (position structure type)
- Called from (representative examples):
  - [tsvectorin](../t/tsvectorin.md) (during tsvector input processing)

## Notes and Other Information
- Returns the new length of the array after duplicate removal
- Modifies the input array in-place for memory efficiency
- Handles complex position merging logic for duplicate entries
- Calculates precise buffer space requirements including alignment
- Critical for tsvector construction from text input in PostgreSQL's full-text search
- Ensures that all positions for identical words are consolidated into single entries
- The function maintains referential integrity between entries and their string data