# removetraverse

## Location
[src/backend/regex/regc_nfa.c:1438-1487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L1438-L1487)

## Overview
The recursive heart of the removeconstraints function that traverses an NFA and removes constraint arcs, replacing them with empty transitions.

## Definition
```c
static void removetraverse(struct nfa *nfa,
                           struct state *s)
```

## Detailed Description
This function recursively traverses the NFA starting from state s, identifying and removing constraint arcs such as lookahead, lookbehind, line anchors, and LACON (lookaround constraint) arcs. These constraint arcs are replaced with empty transitions to maintain the connectivity of the NFA while removing the constraint semantics. The function uses the tmp pointer to mark visited states and prevent infinite loops during traversal.

## Parameters / Member Variables
- `nfa`: The NFA structure being modified
- `s`: The current state being processed during traversal

## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP
  - NERR
  - REG_ETOOBIG
  - NISERR
  - [removetraverse](removetraverse.md) (recursive call)
  - PLAIN, EMPTY, CANTMATCH, AHEAD, BEHIND, LACON (arc type constants)
  - [newarc](../n/newarc.md)
  - [freearc](../f/freearc.md)
  - REG_ASSERT
- Called from (representative examples):
  - removeconstraints
  - [removetraverse](removetraverse.md) (recursive calls)

## Notes and Other Information
The function processes different types of arcs differently: PLAIN, EMPTY, and CANTMATCH arcs are left unchanged, while constraint arcs (AHEAD, BEHIND, '^', '$', LACON) are replaced with empty transitions. This is part of the regex compilation process where constraint handling is simplified by converting constraints to empty transitions. The function includes stack overflow protection and comprehensive error handling throughout the traversal process.

## Simplified Source

```c
static void
removetraverse(struct nfa *nfa, struct state *s)
{
    // Guard against stack overflow in recursive calls
    if (STACK_TOO_DEEP(nfa->v->re)) {
        NERR(REG_ETOOBIG);
        return;
    }

    // Skip if already processed
    if (s->tmp != NULL) return;

    s->tmp = s;  // mark as being processed

    // Process all outgoing arcs
    for (struct arc *a = s->outs; a != NULL && !NISERR(); ) {
        struct arc *next_arc = a->outchain;  // save next before possible freearc

        // Recursively process destination state
        removetraverse(nfa, a->to);
        if (NISERR()) break;

        // Handle different arc types
        switch (a->type) {
            case PLAIN:
            case EMPTY:
            case CANTMATCH:
                // Keep these arcs unchanged
                break;

            case AHEAD:
            case BEHIND:
            case '^':
            case '$':
            case LACON:
                // Replace constraint arcs with empty transitions
                newarc(nfa, EMPTY, 0, s, a->to);
                freearc(nfa, a);
                break;

            default:
                NERR(REG_ASSERT);
                break;
        }

        a = next_arc;
    }
}
```