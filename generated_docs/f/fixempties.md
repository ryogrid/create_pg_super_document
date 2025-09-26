# fixempties

## Location
[src/backend/regex/regc_nfa.c:2076-2302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L2076-L2302)

## Overview
Eliminates EMPTY arcs from the NFA (Non-deterministic Finite Automaton) by consolidating states and creating equivalent non-EMPTY transitions in PostgreSQL's regex engine.

## Definition
```c
static void fixempties(struct nfa *nfa, FILE *f)
```

## Detailed Description
The `fixempties` function is a crucial optimization phase in PostgreSQL's regex compilation that removes EMPTY arcs (epsilon transitions) from the NFA while preserving the language recognized by the automaton. This process involves three main phases:

1. **Single-arc elimination**: Removes states that have only one outgoing EMPTY arc by merging them with their successors, and states with only one incoming EMPTY arc by merging them with their predecessors.

2. **Chain consolidation**: For remaining states, finds all states reachable via chains of EMPTY arcs and creates direct non-EMPTY transitions that bypass these chains. The algorithm chooses to push arcs forward rather than pull them back.

3. **Cleanup**: Removes all remaining EMPTY arcs and any states that have become useless (no inputs or outputs).

The algorithm is designed to avoid O(N³) complexity by only considering original arcs when creating new transitions, using a sophisticated tracking mechanism to identify which arcs existed before the consolidation phase began.

## Parameters / Member Variables
- `nfa`: Pointer to the NFA structure to be optimized
- `f`: File pointer for debug output (can be NULL if no debug output is desired)

## Dependencies
- Functions called/Symbols referenced:
  - NISERR
  - moveins
  - dropstate
  - moveouts  
  - MALLOC
  - NERR
  - REG_ESPACE
  - FREE
  - hasnonemptyout
  - emptyreachable
  - mergeins
  - freearc
  - dumpnfa
  - EMPTY (arc type constant)
- Called from:
  - optimize
  - REPLACEARC

## Notes and Other Information
- The function implements a sophisticated algorithm to avoid O(N³) complexity when dealing with chains of EMPTY arcs
- Uses temporary workspace arrays to accumulate and sort arc collections before merging
- Maintains original arc pointers to distinguish between pre-existing and newly created arcs
- Only processes target states that have non-EMPTY outarcs, as states with only EMPTY outarcs will become useless
- The algorithm's choice to push arcs forward rather than pull them back is somewhat arbitrary but required for consistency
- Debug output can be enabled by providing a non-NULL file pointer
- Located in src/backend/regex/regc_nfa.c:2076-2302