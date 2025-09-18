# searchstoplist

## Location
src/backend/tsearch/ts_utils.c: 140 - 145

## Overview
Performs a binary search to determine if a given word exists in a sorted StopList structure.

## Definition


## Detailed Description
This function efficiently searches for a word in a StopList using binary search algorithm. It leverages the fact that the StopList array was sorted during initialization by  to perform O(log n) lookups. The function performs safety checks to ensure the StopList is valid (non-NULL stop array and positive length) before attempting the search.

The implementation uses the standard C library  function with PostgreSQL's  comparison function to maintain consistency with the sorting performed during list creation.

## Parameters / Member Variables
- : Pointer to the StopList structure to search within
- : The word to search for in the stop list

## Dependencies
- Functions called/Symbols referenced:
  - bsearch (standard C library binary search function)
  - pg_qsort_strcmp (PostgreSQL string comparison function)
  - StopList (structure definition)
- Called from (representative examples):
  - [dsnowball_lexize](../d/dsnowball_lexize.md) (Snowball dictionary lexization)
  - [dispell_lexize](../d/dispell_lexize.md) (Ispell dictionary lexization)
  - [dsimple_lexize](../d/dsimple_lexize.md) (Simple dictionary lexization)

## Notes and Other Information
- Returns true if the word is found in the stop list, false otherwise
- Assumes the StopList has been properly sorted (typically by )
- Performs efficient O(log n) binary search due to sorted array structure
- Includes safety checks for NULL or empty stop lists, returning false in such cases
- The search is case-sensitive and depends on the string comparison used during sorting
- Critical for text search dictionary implementations to filter out common stop words