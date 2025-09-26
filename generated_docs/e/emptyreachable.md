# emptyreachable

## Location
[src/backend/regex/regc_nfa.c:2303-2330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L2303-L2330)

## Overview
Recursively finds all states that can reach a given state through chains of EMPTY arcs in PostgreSQL's regex NFA implementation.

## Definition
```c
static struct state *emptyreachable(struct nfa *nfa, struct state *s, struct state *lastfound, struct arc **inarcsorig)
```

## Detailed Description
The `emptyreachable` function performs a recursive depth-first search to discover all states that can reach a target state `s` through one or more EMPTY (epsilon) transitions. It builds a linked list of reachable states using their `tmp` fields, creating a chain that can be traversed without needing to search the entire NFA.

The function is specifically designed to work with the `fixempties` optimization phase, where it helps identify which states need to have their non-EMPTY arcs propagated forward to eliminate EMPTY arc chains. It only considers original arcs (those that existed before the current optimization phase) by using the `inarcsorig` array.

The function includes stack overflow protection, as the maximum recursion depth equals the length of the longest loop-free chain of EMPTY arcs, which could potentially be as large as the NFA itself.

## Parameters / Member Variables
- `nfa`: Pointer to the NFA structure being analyzed
- `s`: The target state for which to find predecessors reachable via EMPTY arcs
- `lastfound`: The previously found state in the chain (used for building the linked list)
- `inarcsorig`: Array of pointers to original inarcs for each state, indexed by state number

## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP
  - NERR
  - REG_ETOOBIG
  - EMPTY (arc type constant)
  - [emptyreachable](emptyreachable.md) (recursive call)
- Called from:
  - [fixempties](../f/fixempties.md)
  - [emptyreachable](emptyreachable.md) (recursive)
  - REPLACEARC

## Notes and Other Information
- The function builds a linked list through the `tmp` field of states, where each state points to the previously found state
- Returns the last state found in the chain, which serves as the head of the linked list
- Uses the `tmp` field as a visited marker (NULL means unvisited) to avoid infinite loops
- Only examines original inarcs to avoid considering arcs created during the current optimization phase
- Includes stack depth checking to prevent stack overflow on very long EMPTY arc chains
- The recursion depth is bounded by the length of the longest loop-free EMPTY arc chain
- Part of the EMPTY arc elimination system in PostgreSQL's regex engine
- Located in src/backend/regex/regc_nfa.c:2303-2330