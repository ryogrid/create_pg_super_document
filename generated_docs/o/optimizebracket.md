# optimizebracket

## Location
[src/backend/regex/regcomp.c:1937-1992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L1937-L1992)

## Overview
Optimizes bracket expressions by converting certain patterns (like "[\s\S]") into a single RAINBOW arc, which represents all possible character matches for improved efficiency.

## Definition

```c
static void
optimizebracket(struct vars *v,
				struct state *lp,
				struct state *rp)
```
## Detailed Description
The optimizebracket function analyzes bracket expressions in regular expressions to identify cases where all possible colors (character classes) are covered by the expression. When such patterns are detected, the function replaces multiple individual arcs with a single RAINBOW arc, which is more efficient to process. This optimization is particularly useful for patterns like "[\s\S]" which effectively match any character but are written in a verbose form common in some regex flavors.

The function works by:
1. Scanning all outgoing arcs from the left state to mark which colors are referenced
2. Checking if all available colors in the color map are covered by these arcs
3. If all colors are covered, replacing the multiple arcs with a single RAINBOW arc

## Parameters / Member Variables
- `*v`: Pointer to the vars structure containing regex compilation state and context
- `*lp`: Pointer to the left state (source state) of the bracket expression
- `*rp`: Pointer to the right state (destination state) of the bracket expression
## Dependencies
- Functions called/Symbols referenced:
  - [CDEND](../C/CDEND.md) (macro for color descriptor end)
  - UNUSEDCOLOR (macro to check if color is unused)
  - [freearc](../f/freearc.md) (function to free an arc)
  - [newarc](../n/newarc.md) (function to create a new arc)
- Data structures used:
  - colordesc (color descriptor structure)
  - [arc](../a/arc.md) (arc structure)
- Constants used:
  - PLAIN, RAINBOW, PSEUDO, COLMARK (arc and color flags)
- Called from:
  - [bracket](../b/bracket.md) (in regcomp.c:1717)

## Notes and Other Information
- This function assumes all input arcs are PLAIN type arcs pointing to the same destination state
- The optimization only applies when ALL colors in the color map are covered by the bracket expression
- Uses transient marking (COLMARK flag) to track which colors are referenced during analysis
- The resulting RAINBOW arc is semantically equivalent to the original set of arcs but more efficient to process
- This optimization handles patterns that might seem redundant but are common in some regex dialects

## Simplified Source

```c
static void optimizebracket(struct vars *v, struct state *lp, struct state *rp) {
    struct colordesc *cd;
    struct colordesc *end = CDEND(v->cm);
    struct arc *a;
    bool israinbow;

    // Mark all colors referenced by outgoing arcs
    for (a = lp->outs; a != NULL; a = a->outchain) {
        cd = &v->cm->cd[a->co];
        cd->flags |= COLMARK;
    }

    // Check if all colors are covered
    israinbow = true;
    for (cd = v->cm->cd; cd < end; cd++) {
        if (cd->flags & COLMARK)
            cd->flags &= ~COLMARK;  // Clear mark
        else if (!UNUSEDCOLOR(cd) && !(cd->flags & PSEUDO))
            israinbow = false;      // Found uncovered color
    }

    // If all colors covered, replace with single RAINBOW arc
    if (israinbow) {
        while ((a = lp->outs) != NULL)
            freearc(v->nfa, a);
        newarc(v->nfa, PLAIN, RAINBOW, lp, rp);
    }
}
```