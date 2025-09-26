# push

## Location
[src/backend/regex/regc_nfa.c:1891-1986](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L1891-L1986)

## Overview
Pushes a forward constraint forward past its destination state in the PostgreSQL regex NFA (Non-deterministic Finite Automaton) implementation.

## Definition

```c
static int
push(struct nfa *nfa,
	 struct arc *con,
	 struct state **intermediates)
```
## Detailed Description
The  function is a core component of PostgreSQL's regex engine constraint optimization. It takes a forward constraint arc and propagates it through its destination state to subsequent states. This operation is essential for optimizing regular expression matching by moving constraints to more advantageous positions in the NFA.

The function maintains several important invariants:
- It never deletes pre-existing states
- It only removes the given constraint arc from the destination state's inarcs
- It may leave useless states behind, which are cleaned up by the calling  function
- It creates intermediate states as needed to handle multiple constraint combinations

The function returns 1 on success (which occurs unless the destination is the post state or an internal error occurs) and 0 if no operation was performed.

## Parameters / Member Variables
- : Pointer to the NFA structure being modified
- : The constraint arc to be pushed forward
- : Pointer to a list of intermediate states for the destination state, chained through their tmp fields

## Dependencies
- Functions called/Symbols referenced:
  - [freearc](../f/freearc.md)
  - [newstate](../n/newstate.md)
  - NISERR
  - [copyouts](../c/copyouts.md)
  - [cparc](../c/cparc.md)
  - combine
  - [newarc](../n/newarc.md)
  - [moveouts](../m/moveouts.md)
  - INCOMPATIBLE, SATISFIED, COMPATIBLE, REPLACEARC (enum values)
  - NOTREACHED
- Called from:
  - [pushfwd](pushfwd.md)

## Notes and Other Information
- The function handles multiple scenarios when combining constraints with outgoing arcs: INCOMPATIBLE (destroys arc), SATISFIED (no action), COMPATIBLE (creates intermediate state), and REPLACEARC (replaces arc color)
- When the destination state has multiple input arcs, the function clones the state to avoid affecting other inarcs
- Intermediate states are reused when possible to avoid creating duplicate states for the same predecessor-successor combinations
- Part of the regex constraint optimization system in PostgreSQL's regex engine located in src/backend/regex/regc_nfa.c:1891-1986