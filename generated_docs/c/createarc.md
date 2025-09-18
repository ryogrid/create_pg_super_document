# createarc

## Location
[src/backend/regex/regc_nfa.c:323-367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L323-L367)

## Overview
Creates a new arc within an NFA by allocating memory and linking it into the state transition chains, but only after duplicate checking has been performed.

## Definition
```c
static void createarc(struct nfa *nfa, int t, color co, struct state *from, struct state *to)
```

## Detailed Description
The createarc function performs the actual creation of a new arc within an NFA structure after duplicate checking has been completed by newarc. It allocates memory for the new arc using allocarc, initializes the arc's properties, and links it into both the outgoing chain of the source state and the incoming chain of the destination state. The function uses a bidirectional linked list structure for efficient arc management and updates the arc count for both states. If the arc is colored and the NFA has no parent, it also adds the arc to the color chain for color management.

## Parameters / Member Variables
- `nfa`: Pointer to the NFA structure that will contain the new arc
- `t`: The type of the arc being created
- `co`: The color associated with the arc (used in regex character classification)
- `from`: Pointer to the source state of the arc
- `to`: Pointer to the destination state of the arc

## Dependencies
- Functions called/Symbols referenced:
  - [allocarc](../a/allocarc.md) (for arc memory allocation)
  - NISERR (for error checking)
  - COLORED (macro to check if arc is colored)
  - [colorchain](colorchain.md) (for color chain management)
- Called from (representative examples):
  - [newarc](../n/newarc.md) (main arc creation entry point)
  - [moveins](../m/moveins.md) (moving incoming arcs)
  - [copyins](copyins.md) (copying incoming arcs)
  - [copyouts](copyouts.md) (copying outgoing arcs)
  - [mergeins](../m/mergeins.md) (merging incoming arcs)

## Notes and Other Information
- This function must only be called after verifying no duplicate arc exists
- Arcs are added to the beginning of chains rather than the end for simplicity
- The function maintains bidirectional linked lists for both incoming and outgoing arc chains
- Updates state arc counters (nouts and nins) for proper bookkeeping
- Handles color chain management for colored arcs in parent NFAs
- Uses reverse chain pointers (inchainRev, outchainRev) for efficient bidirectional traversal