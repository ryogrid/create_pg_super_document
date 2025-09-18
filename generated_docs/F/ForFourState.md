# ForFourState

## Location
src/include/nodes/pg_list.h: 102 - 109

## Overview
ForFourState is a state structure used to maintain iteration state when traversing four PostgreSQL lists simultaneously in parallel.

## Definition


## Detailed Description
ForFourState is a utility structure designed to support parallel iteration over four PostgreSQL List structures. It maintains references to four lists and tracks the current position (index) across all lists simultaneously. This structure is primarily used internally by the  macro to enable convenient iteration over multiple lists where corresponding elements at the same index are processed together. The structure ensures that all four lists are traversed in lockstep, maintaining synchronization across the iteration.

## Parameters / Member Variables
- : Pointer to the first list being iterated
- : Pointer to the second list being iterated  
- : Pointer to the third list being iterated
- : Pointer to the fourth list being iterated
- : Current common index position across all four lists

## Dependencies
- Functions called/Symbols referenced:
  - List (PostgreSQL list structure)
- Called from (representative examples):
  - forfour macro (via initialization in src/include/nodes/pg_list.h:576)

## Notes and Other Information
- This structure is part of PostgreSQL's list iteration infrastructure
- Designed for internal use by the forfour macro rather than direct manipulation
- Enables efficient parallel traversal of multiple lists with guaranteed synchronization
- Similar structures exist for different numbers of lists (ForThreeState, ForFiveState)
- The structure assumes all lists have compatible lengths for meaningful parallel iteration