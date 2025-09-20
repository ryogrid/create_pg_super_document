# ForFourState

## Location
[src/include/nodes/pg_list.h:102-109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pg_list.h#L102-L109)

## Overview
ForFourState is a state structure used to maintain iteration state when traversing four PostgreSQL lists simultaneously in parallel.

## Definition

```c
typedef struct ForFourState
{
	const List *l1;				/* lists we're looping through */
	const List *l2;
	const List *l3;
	const List *l4;
	int			i;				/* common element index */
} ForFourState;
```
## Detailed Description
ForFourState is a utility structure designed to support parallel iteration over four PostgreSQL List structures. It maintains references to four lists and tracks the current position (index) across all lists simultaneously. This structure is primarily used internally by the  macro to enable convenient iteration over multiple lists where corresponding elements at the same index are processed together. The structure ensures that all four lists are traversed in lockstep, maintaining synchronization across the iteration.

## Parameters / Member Variables
- `*l1`: Pointer to the first list being iterated
- `*l2`: Pointer to the second list being iterated
- `*l3`: Pointer to the third list being iterated
- `*l4`: Pointer to the fourth list being iterated
- `i`: Current common index position across all four lists
## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (PostgreSQL list structure)
- Called from (representative examples):
  - forfour macro (via initialization in src/include/nodes/pg_list.h:576)

## Notes and Other Information
- This structure is part of PostgreSQL's list iteration infrastructure
- Designed for internal use by the forfour macro rather than direct manipulation
- Enables efficient parallel traversal of multiple lists with guaranteed synchronization
- Similar structures exist for different numbers of lists (ForThreeState, ForFiveState)
- The structure assumes all lists have compatible lengths for meaningful parallel iteration