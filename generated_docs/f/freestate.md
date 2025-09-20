# freestate

## Location
[src/backend/regex/regc_nfa.c:242-280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L242-L280)

## Overview
Deallocates a state that has no incoming or outgoing arcs by removing it from the NFA's state list and adding it to the free list for reuse.

## Definition

```c
static void
freestate(struct nfa *nfa,
		  struct state *s)
```
## Detailed Description
The  function performs controlled deallocation of an NFA state that must have no connected arcs. It first verifies the state has no incoming or outgoing arcs through assertions. The function then removes the state from the NFA's doubly-linked state list by updating the prev/next pointers of neighboring states, and adjusts the NFA's states and slast pointers if necessary. Rather than immediately freeing the memory, it adds the state to the NFA's freestates list for efficient reuse by future  calls.

## Parameters / Member Variables
- : Pointer to the NFA structure containing the state
- : Pointer to the state to be freed (must have no arcs)

## Dependencies
- Functions called/Symbols referenced:
  - FREESTATE
- Called from (representative examples):
  - [dropstate](../d/dropstate.md) (in regc_nfa.c)
  - [deltraverse](../d/deltraverse.md) (in regc_nfa.c)
  - breakconstraintloop (in regc_nfa.c)
  - [cbracket](../c/cbracket.md) (in regcomp.c)

## Notes and Other Information
- Requires that the state has no incoming or outgoing arcs (enforced by assertions)
- Implements memory recycling by maintaining a freestates list rather than immediate deallocation
- Properly maintains the doubly-linked list integrity of the NFA's state list
- Sets state number to FREESTATE constant to mark it as freed
- Critical for memory efficiency during NFA construction and optimization
- State memory is reused by newstate function for better performance