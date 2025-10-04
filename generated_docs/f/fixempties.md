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
  - [moveins](../m/moveins.md)
  - [dropstate](../d/dropstate.md)
  - [moveouts](../m/moveouts.md)  
  - MALLOC
  - NERR
  - REG_ESPACE
  - FREE
  - [hasnonemptyout](../h/hasnonemptyout.md)
  - [emptyreachable](../e/emptyreachable.md)
  - [mergeins](../m/mergeins.md)
  - [freearc](freearc.md)
  - [dumpnfa](../d/dumpnfa.md)
  - EMPTY (arc type constant)
- Called from:
  - [optimize](../o/optimize.md)
  - REPLACEARC

## Notes and Other Information
- The function implements a sophisticated algorithm to avoid O(N³) complexity when dealing with chains of EMPTY arcs
- Uses temporary workspace arrays to accumulate and sort arc collections before merging
- Maintains original arc pointers to distinguish between pre-existing and newly created arcs
- Only processes target states that have non-EMPTY outarcs, as states with only EMPTY outarcs will become useless
- The algorithm's choice to push arcs forward rather than pull them back is somewhat arbitrary but required for consistency
- Debug output can be enabled by providing a non-NULL file pointer
- Located in src/backend/regex/regc_nfa.c:2076-2302

## Simplified Source
```c
static void fixempties(struct nfa *nfa, FILE *f) {
    struct state *s, *s2, *nexts;
    struct arc *a, *nexta;
    struct arc **inarcsorig, **arcarray;
    int totalinarcs, arccount, prevnins, nskip;

    // Phase 1: Remove states with single EMPTY out-arc
    for (s = nfa->states; s != NULL && !NISERR(); s = nexts) {
        nexts = s->next;
        if (s->flag || s->nouts != 1) continue;

        a = s->outs;
        if (a->type != EMPTY) continue;

        if (s != a->to)
            moveins(nfa, s, a->to);
        dropstate(nfa, s);
    }

    // Phase 2: Remove states with single EMPTY in-arc
    for (s = nfa->states; s != NULL && !NISERR(); s = nexts) {
        nexts = s->next;
        if (s->flag || s->nins != 1) continue;

        a = s->ins;
        if (a->type != EMPTY) continue;

        if (s != a->from)
            moveouts(nfa, s, a->from);
        dropstate(nfa, s);
    }

    // Phase 3: Handle complex EMPTY chains
    // Allocate workspace for original arcs tracking
    inarcsorig = MALLOC(nfa->nstates * sizeof(struct arc *));
    totalinarcs = 0;
    for (s = nfa->states; s != NULL; s = s->next) {
        inarcsorig[s->no] = s->ins;
        totalinarcs += s->nins;
    }

    arcarray = MALLOC(totalinarcs * sizeof(struct arc *));

    // Process each target state
    for (s = nfa->states; s != NULL && !NISERR(); s = s->next) {
        // Skip states without non-EMPTY outarcs
        if (!s->flag && !hasnonemptyout(s)) continue;

        // Find all states reachable via EMPTY arcs
        arccount = 0;
        for (s2 = emptyreachable(nfa, s, s, inarcsorig); s2 != s; s2 = nexts) {
            // Collect non-EMPTY arcs from reachable states
            for (a = inarcsorig[s2->no]; a != NULL; a = a->inchain) {
                if (a->type != EMPTY)
                    arcarray[arccount++] = a;
            }
            nexts = s2->tmp;
            s2->tmp = NULL;
        }
        s->tmp = NULL;

        prevnins = s->nins;
        mergeins(nfa, s, arcarray, arccount);

        // Update original arcs pointer
        nskip = s->nins - prevnins;
        a = s->ins;
        while (nskip-- > 0) a = a->inchain;
        inarcsorig[s->no] = a;
    }

    FREE(arcarray);
    FREE(inarcsorig);

    // Phase 4: Remove all EMPTY arcs
    for (s = nfa->states; s != NULL; s = s->next) {
        for (a = s->outs; a != NULL; a = nexta) {
            nexta = a->outchain;
            if (a->type == EMPTY)
                freearc(nfa, a);
        }
    }

    // Phase 5: Remove useless states
    for (s = nfa->states; s != NULL; s = nexts) {
        nexts = s->next;
        if ((s->nins == 0 || s->nouts == 0) && !s->flag)
            dropstate(nfa, s);
    }
}
```