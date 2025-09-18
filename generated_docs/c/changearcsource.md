# changearcsource

## Location
[src/backend/regex/regc_nfa.c:489-532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L489-L532)

## Overview
Changes the source state of an existing arc by unlinking it from the old source state and linking it to a new source state.

## Definition
```c
static void changearcsource(struct arc *a, struct state *newfrom)
```

## Detailed Description
The changearcsource function modifies an existing arc to have a different source state. It carefully maintains the bidirectional linked list structure by first removing the arc from the old source state's outgoing arc chain, updating the arc's from pointer, and then adding it to the new source state's outgoing arc chain. The function ensures proper maintenance of arc counters and chain linkages throughout the operation. This operation is used during NFA transformations and optimizations where arc routing needs to be changed.

## Parameters / Member Variables
- `a`: Pointer to the arc whose source state needs to be changed
- `newfrom`: Pointer to the new source state for the arc

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls - only direct structure manipulation)
- Called from (representative examples):
  - [moveouts](../m/moveouts.md) (when moving outgoing arcs between states)

## Notes and Other Information
- Caller must verify that no duplicate arc will be created by this operation
- Maintains bidirectional linked list integrity during the transfer
- Updates arc counters (nouts) for both old and new source states
- Uses assertions to verify data structure consistency
- The arc is prepended to the new source's outgoing chain for efficiency
- Only modifies the source side of the arc - destination state remains unchanged
- Assumes the caller has verified that oldfrom != newfrom
- Does not handle color chain updates (if needed, caller must handle separately)
- Used primarily in NFA optimization and state merging operations