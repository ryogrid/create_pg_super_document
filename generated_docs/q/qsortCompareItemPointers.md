# qsortCompareItemPointers

## Location
src/backend/access/gin/ginbulk.c: 246 - 256

## Overview
A static comparison function used as a callback for qsort operations on ItemPointer arrays, ensuring no duplicate ItemPointers exist during sorting.

## Definition


## Detailed Description
This function serves as a comparison callback for the standard C library qsort function when sorting arrays of ItemPointer structures. It wraps the PostgreSQL-specific ginCompareItemPointers function to provide the exact interface required by qsort (accepting void* parameters and returning an integer comparison result).

The function includes an important assertion that verifies no two ItemPointers being sorted are equal, which helps catch data integrity issues during GIN index construction where duplicate heap pointers should not occur.

## Parameters / Member Variables
- : First ItemPointer to compare (passed as void* by qsort)
- : Second ItemPointer to compare (passed as void* by qsort)

## Dependencies
- Functions called/Symbols referenced:
  - ginCompareItemPointers (underlying comparison function)
- Called from:
  - ginGetBAEntry (used in qsort call)

## Notes and Other Information
- Static function with file-local scope
- Designed specifically for use with the qsort standard library function
- Contains an assertion to detect duplicate ItemPointers, which would indicate a bug in the bulk insertion logic
- The assertion helps ensure data integrity during GIN index construction
- Returns the same comparison semantics as ginCompareItemPointers (negative, zero, or positive integer)