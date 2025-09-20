# copyouts

## Location
[src/backend/regex/regc_nfa.c:1167-1255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L1167-L1255)

## Overview
Copies all outgoing arcs from one state to another state without removing arcs from the source state.

## Definition

```c
static void
copyouts(struct nfa *nfa,
		 struct state *oldState,
		 struct state *newState)
```
## Detailed Description
The `copyouts` function creates copies of all outgoing arcs from an old state and adds them to a new state. Unlike `moveouts`, this function preserves the original arcs in the source state. The function is designed to work efficiently when the new state has no existing outgoing arcs (which is asserted). The implementation includes conditional compilation sections for more complex deduplication logic, but the active code path assumes the new state starts with no outgoing arcs, simplifying the operation to a straightforward copy process.

The function iterates through all outgoing arcs of the old state and creates corresponding new arcs in the new state with the same type, color, and destination.

## Parameters / Member Variables
- `nfa`: Pointer to the NFA structure being modified
- `oldState`: Source state from which outgoing arcs will be copied (must be different from newState)
- `newState`: Destination state that will receive copies of the outgoing arcs (must have zero existing outgoing arcs)

## Dependencies
- Functions called/Symbols referenced:
  - [createarc](createarc.md)
  - BULK_ARC_OP_USE_SORT (in conditional compilation sections)
  - [cparc](cparc.md) (in conditional compilation sections)
  - INTERRUPT (in conditional compilation sections)
  - [sortouts](../s/sortouts.md) (in conditional compilation sections)
  - NISERR (in conditional compilation sections)
  - [sortouts_cmp](../s/sortouts_cmp.md) (in conditional compilation sections)
  - NOTREACHED (in conditional compilation sections)
- Called from (representative examples):
  - push
  - [makesearch](../m/makesearch.md)

## Notes and Other Information
- The function assumes newState has no existing outgoing arcs (assertion check)
- Contains conditional compilation sections (\#ifdef NOT_USED) with more complex deduplication logic
- The active implementation is optimized for the common case where the destination state is empty
- Part of the NFA state manipulation utilities for regex compilation
- Located in src/backend/regex/regc_nfa.c:1167-1255