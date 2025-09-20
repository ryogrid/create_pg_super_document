# dropstate

## Location
[src/backend/regex/regc_nfa.c:226-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L226-L241)

## Overview
Completely removes a state from an NFA by freeing all its incoming and outgoing arcs, then deallocating the state itself.

## Definition

```c
static void
dropstate(struct nfa *nfa,
		  struct state *s)
```
## Detailed Description
The  function performs complete removal of a state from an NFA structure. It systematically deletes all arcs connected to the state by iterating through the state's incoming arcs (ins) and outgoing arcs (outs), calling  for each one. After all arcs are removed, it calls  to deallocate the state itself. This ensures proper cleanup and prevents memory leaks when states need to be removed during NFA optimization or error handling.

## Parameters / Member Variables
- : Pointer to the NFA structure containing the state
- : Pointer to the state to be dropped/removed

## Dependencies
- Functions called/Symbols referenced:
  - [freearc](../f/freearc.md)
  - [freestate](../f/freestate.md)
- Called from (representative examples):
  - [pullback](../p/pullback.md) (in regc_nfa.c)
  - [pushfwd](../p/pushfwd.md) (in regc_nfa.c)
  - fixempties (in regc_nfa.c)
  - fixconstraintloops (in regc_nfa.c)
  - clonesuccessorstates (in regc_nfa.c)
  - [cleanup](../c/cleanup.md) (in regc_nfa.c)
  - [charclasscomplement](../c/charclasscomplement.md) (in regcomp.c)
  - [cbracket](../c/cbracket.md) (in regcomp.c)

## Notes and Other Information
- Essential for NFA optimization and cleanup operations
- Must be called carefully as it completely removes the state from the NFA
- Used during NFA simplification processes like empty state removal
- Critical for maintaining NFA integrity during transformations
- The state becomes invalid after this call and should not be referenced again