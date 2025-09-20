# checkAllTheSame

## Location
[src/backend/access/spgist/spgdoinsert.c:599-676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L599-L676)

## Overview
This function detects when a user-defined picksplit function fails to properly distribute leaf tuples across multiple nodes and randomly redistributes them to enable "allTheSame" mode in SPGiST.

## Definition

```c
static bool
checkAllTheSame(spgPickSplitIn *in, spgPickSplitOut *out, bool tooBig,
				bool *includeNew)
```
## Detailed Description
This function serves as a failsafe mechanism for SPGiST index operations when the user-defined picksplit function produces inadequate node distribution. It detects the problematic case where all tuples are assigned to the same node, which would not achieve the desired space-partitioning effect.

When such a situation is detected, the function:
1. Overrides the picksplit function's decisions
2. Creates 8 arbitrary child nodes
3. Randomly distributes tuples across these nodes using modulo assignment
4. Preserves node labels if they exist by duplicating the original label
5. Returns true to signal that "allTheSame" mode should be used

The function includes special handling for cases where the tuple set is too large to fit on one page, excluding the new (incoming) tuple from the distribution check to avoid infinite loops.

## Parameters / Member Variables
- : Input structure containing the tuples to be split and related information
- : Output structure that will be modified to contain the new node assignments
- : Boolean indicating if the tuple set is too large to fit on one page
- : Output parameter indicating whether the new tuple should be included in the split

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation for node labels array)
- Called from (representative examples):
  - [doPickSplit](../d/doPickSplit.md) (at src/backend/access/spgist/spgdoinsert.c:900)

## Notes and Other Information
- Uses an arbitrary number of 8 child nodes for redistribution
- Random distribution is achieved using modulo operator (i % out->nNodes)
- Special case handling for scenarios where all existing tuples fit in one node but adding the new tuple would exceed page capacity
- When tooBig is true and the new tuple is in its own node, it's excluded from the split (*includeNew = false)
- Preserves existing node labels by duplicating them across all new nodes
- Does not modify prefix or leaf tuple datum assignments from the original picksplit output
- This mechanism prevents infinite loops that could occur with poorly implemented picksplit functions
- Also used to forcibly select allTheSame mode for null values
- Location: src/backend/access/spgist/spgdoinsert.c:599-676