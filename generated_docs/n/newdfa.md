# newdfa

## Location
[src/backend/regex/rege_dfa.c:607-690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/rege_dfa.c#L607-L690)

## Overview
Creates and initializes a fresh DFA structure for regex execution, handling both small and large DFA configurations with appropriate memory allocation.

## Definition
```c
static struct dfa *
newdfa(struct vars *v,
       struct cnfa *cnfa,
       struct colormap *cm,
       struct smalldfa *sml)  /* preallocated space, may be NULL */
```

## Detailed Description
The `newdfa` function allocates and initializes a new DFA structure for regex execution. It implements a two-tier allocation strategy: small DFAs (with few states and colors) use a pre-allocated `smalldfa` structure for efficiency, while larger DFAs get individual heap allocations. The function sets up all necessary arrays for state sets, state representations, transition outputs, and incoming arc tracking. It also configures size limits, backref information, and other execution parameters based on the provided CNFA and execution flags.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing regex execution context and flags
- `cnfa`: Pointer to compiled NFA structure that defines the automaton
- `cm`: Pointer to colormap structure for character classification
- `sml`: Optional pointer to pre-allocated smalldfa space for efficiency

## Dependencies
- Functions called/Symbols referenced:
  - MALLOC (memory allocation macro)
  - ERR (error reporting macro)
  - [freedfa](../f/freedfa.md) (cleanup function called on allocation failure)
  - REG_ESPACE, REG_SMALL (error and flag constants)
  - FEWSTATES, FEWCOLORS, UBITS, WORK (size and allocation constants)
- Called from (representative examples):
  - LOFF (regex execution setup)
  - [getsubdfa](../g/getsubdfa.md), getladfa (DFA retrieval functions)
  - [find](../f/find.md) (main search function)
  - [cfind](../c/cfind.md) (complex find function)

## Notes and Other Information
- Uses smalldfa optimization for DFAs with ≤FEWSTATES states and ≤FEWCOLORS colors
- Implements memory-efficient allocation strategy with proper cleanup on failure
- Sets up state set arrays, bit vector work areas, and transition tables
- Configures execution limits based on REG_SMALL flag (7 vs full state sets)
- Initializes backref tracking (backno=-1 initially, may be set by caller)
- Critical infrastructure component for all DFA-based regex execution in PostgreSQL
- Returns initialized DFA pointer on success, NULL with error set on failure