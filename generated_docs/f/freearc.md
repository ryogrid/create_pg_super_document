# freearc

## Location
[src/backend/regex/regc_nfa.c:418-488](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L418-L488)

## Overview
Frees an arc from an NFA by unlinking it from state chains, cleaning up color chains, and adding it to the freelist for reuse.

## Definition
```c
static void freearc(struct nfa *nfa, struct arc *victim)
```

## Detailed Description
The freearc function safely removes an arc from an NFA structure by performing careful cleanup operations. It handles bidirectional linked list maintenance for both incoming and outgoing arc chains, removes the arc from color chains if necessary, updates state arc counters, and clears all arc fields before adding it to the NFA's freelist for potential reuse. The function uses extensive assertions to ensure data structure integrity during the unlinking process and takes precautions to clear all pointers to prevent dangling references.

## Parameters / Member Variables
- `nfa`: Pointer to the NFA structure containing the arc to be freed
- `victim`: Pointer to the arc structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - COLORED (macro to check if arc is colored)
  - [uncolorchain](../u/uncolorchain.md) (removes arc from color chain)
- Called from (representative examples):
  - [dropstate](../d/dropstate.md) (when removing states and their arcs)
  - [moveins](../m/moveins.md) (when reorganizing incoming arcs)
  - [moveouts](../m/moveouts.md) (when reorganizing outgoing arcs)
  - [pull](../p/pull.md)/push operations (during NFA optimization)
  - [fixempties](fixempties.md) (when fixing empty transitions)
  - [optimizebracket](../o/optimizebracket.md) (during bracket optimization)

## Notes and Other Information
- Maintains bidirectional linked list integrity for both incoming and outgoing arc chains
- Handles color chain management for colored arcs in parent NFAs
- Updates state arc counters (nouts, nins) to maintain accurate bookkeeping
- Clears all arc fields as a precaution against dangling pointer usage
- Adds freed arcs to the NFA's freelist for memory recycling via allocarc
- Uses extensive assertions to verify data structure consistency
- The arc type is set to 0 to mark it as freed/invalid
- Memory recycling through the freelist improves allocation performance
- Careful pointer management prevents memory corruption during unlinking operations