# arc

## Location
[src/include/regex/regguts.h:295-302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/regex/regguts.h#L295-L302)

## Overview
A structure representing a labeled transition (edge) in PostgreSQL's regex Nondeterministic Finite Automaton (NFA), connecting two states and specifying what characters/colors trigger the transition.

## Definition
```c
struct arc
{
    int         type;           /* 0 if free, else an NFA arc type code */
    color       co;             /* color the arc matches (possibly RAINBOW) */
    struct state *from;         /* where it's from */
    struct state *to;           /* where it's to */
    struct arc *outchain;       /* link in *from's outs chain or free chain */
    struct arc *outchainRev;    /* back-link in *from's outs chain */
#define freechain   outchain    /* we do not maintain "freechainRev" */
    struct arc *inchain;        /* link in *to's ins chain */
    struct arc *inchainRev;     /* back-link in *to's ins chain */
    /* these fields are not used when co == RAINBOW: */
    struct arc *colorchain;     /* link in color's arc chain */
    struct arc *colorchainRev;  /* back-link in color's arc chain */
};
```

## Detailed Description
The `arc` structure is a fundamental component of PostgreSQL's regex NFA implementation, representing a directed edge between two states that specifies what input characters can trigger a state transition. Each arc has a type (indicating what kind of transition it represents), a color that defines which characters it matches, and pointers to source and destination states.

The structure maintains multiple linked list chains for efficient navigation:
- **State chains**: Links to all outgoing arcs from the source state and incoming arcs to the destination state
- **Color chains**: Links to all arcs that match the same color (not used for RAINBOW arcs)
- **Free chains**: When arcs are freed, they're linked together for memory reuse

The bidirectional chain pointers allow efficient insertion, deletion, and traversal operations on these lists. The color-based chains enable optimizations during regex compilation and execution.

## Parameters / Member Variables
- `type`: Arc type code (0 for free/unused arcs, otherwise an NFA arc type indicating the transition nature)
- `co`: Color that this arc matches, determining which input characters trigger the transition (may be RAINBOW for special cases)
- `from`: Pointer to the source state of this transition
- `to`: Pointer to the destination state of this transition
- `outchain`: Forward link in the source state's outgoing arcs chain, or free chain when arc is unused
- `outchainRev`: Backward link in the source state's outgoing arcs chain for bidirectional traversal
- `inchain`: Forward link in the destination state's incoming arcs chain
- `inchainRev`: Backward link in the destination state's incoming arcs chain
- `colorchain`: Forward link in the color's arc chain (unused for RAINBOW arcs)
- `colorchainRev`: Backward link in the color's arc chain (unused for RAINBOW arcs)

## Dependencies
- Functions called/Symbols referenced:
  - color (color type for character matching)
  - [state](../s/state.md) (state structure for NFA nodes)
  - [arc](arc.md) (self-reference for linked list chains)

- Called from (representative examples):
  - [newarc](../n/newarc.md), createarc, allocarc (arc creation)
  - [freearc](../f/freearc.md) (arc cleanup)
  - [changearcsource](../c/changearcsource.md), changearctarget (arc modification)
  - [findarc](../f/findarc.md), cparc (arc searching and copying)
  - [sortins](../s/sortins.md), sortouts (arc sorting)
  - [moveins](../m/moveins.md), moveouts, copyins, copyouts (arc manipulation)
  - Various NFA analysis and optimization functions
  - Regex compilation and execution functions

## Notes and Other Information
- Central to PostgreSQL's regex NFA representation and enables efficient state machine execution
- The multiple linked list chains provide O(1) insertion/deletion while maintaining fast traversal
- Color-based organization enables optimizations during regex matching by grouping arcs with similar character requirements
- RAINBOW arcs represent special transitions that don't participate in color-specific optimizations
- The freechain macro reuses the outchain field for memory management when arcs are deallocated
- Critical for regex pattern matching performance, as the NFA traversal follows these arcs during input processing
- Part of a sophisticated memory management system that pools freed arcs for reuse
- Used extensively in NFA construction, optimization, and execution phases of regex processing