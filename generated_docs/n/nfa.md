# nfa

## Location
[src/include/regex/regguts.h:348-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/regex/regguts.h#L348-L399)

## Overview
The `nfa` structure represents a Non-deterministic Finite Automaton in PostgreSQL's regular expression engine, containing all states, transitions, and management information needed to represent and process regular expression patterns.

## Definition
```c
struct nfa
{
    struct state *pre;          /* pre-initial state */
    struct state *init;         /* initial state */
    struct state *final;        /* final state */
    struct state *post;         /* post-final state */
    int         nstates;        /* for numbering states */
    struct state *states;       /* chain of live states */
    struct state *slast;        /* tail of the chain */
    struct state *freestates;   /* chain of free states */
    struct arc *freearcs;       /* chain of free arcs */
    struct statebatch *lastsb;  /* chain of statebatches */
    struct arcbatch *lastab;    /* chain of arcbatches */
    size_t      lastsbused;     /* number of states consumed from *lastsb */
    size_t      lastabused;     /* number of arcs consumed from *lastab */
    struct colormap *cm;        /* the color map */
    color       bos[2];         /* colors, if any, assigned to BOS and BOL */
    color       eos[2];         /* colors, if any, assigned to EOS and EOL */
    int         flags;          /* flags to pass forward to cNFA */
    int         minmatchall;    /* min number of chrs to match, if matchall */
    int         maxmatchall;    /* max number of chrs to match, or DUPINF */
    struct vars *v;             /* simplifies compile error reporting */
    struct nfa *parent;         /* parent NFA, if any */
};
```

## Detailed Description
The `nfa` structure is the central data structure in PostgreSQL's regex engine, representing a complete Non-deterministic Finite Automaton. It maintains the graph structure through chains of states and arcs, manages memory through batch allocation systems, handles character classification through color maps, and supports nested NFA structures through parent relationships. The NFA includes special states (pre-initial, initial, final, post-final) that facilitate regex processing and optimization.

## Parameters / Member Variables
- `pre`: Pointer to the pre-initial state (used for anchoring and optimization)
- `init`: Pointer to the initial/starting state of the NFA
- `final`: Pointer to the final/accepting state of the NFA
- `post`: Pointer to the post-final state (used for anchoring and optimization)
- `nstates`: Counter for numbering states within the NFA
- `states`: Head of the chain containing all live/active states
- `slast`: Tail pointer for the live states chain
- `freestates`: Head of the chain containing reusable free states
- `freearcs`: Head of the chain containing reusable free arcs
- `lastsb`: Pointer to the current statebatch being used for state allocation
- `lastab`: Pointer to the current arcbatch being used for arc allocation
- `lastsbused`: Number of states consumed from the current statebatch
- `lastabused`: Number of arcs consumed from the current arcbatch
- `cm`: Pointer to the colormap structure for character classification
- `bos`: Color assignments for Beginning of String (BOS) and Beginning of Line (BOL)
- `eos`: Color assignments for End of String (EOS) and End of Line (EOL)
- `flags`: Compilation flags to pass to the compiled NFA (cNFA)
- `minmatchall`: Minimum number of characters to match for matchall patterns
- `maxmatchall`: Maximum number of characters to match for matchall patterns (or DUPINF for unlimited)
- `v`: Pointer to variables structure for compile error reporting
- `parent`: Pointer to parent NFA for nested constructs

## Dependencies
- Functions called/Symbols referenced:
  - `state` (for pre, init, final, post, states, slast, freestates)
  - `arc` (for freearcs chain)
  - `statebatch` (for lastsb batch allocation)
  - `arcbatch` (for lastab batch allocation)
  - `colormap` (for cm character classification)
  - `color` (for bos/eos arrays)
- Called from (representative examples):
  - `newnfa` (for NFA creation and initialization)
  - `freenfa` (for NFA cleanup and deallocation)
  - Various regex compilation and optimization functions

## Notes and Other Information
- Central data structure of PostgreSQL's regex engine located in src/include/regex/regguts.h
- Uses batch allocation systems (statebatch/arcbatch) for efficient memory management
- Supports nested NFAs through parent relationships for complex regex constructs
- The four special states (pre, init, final, post) enable various regex optimizations and anchoring
- Color mapping system allows efficient character class handling
- Free chains enable memory reuse for better performance
- The structure supports both simple and complex regex patterns through its flexible design