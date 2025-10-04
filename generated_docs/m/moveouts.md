# moveouts

## Location
[src/backend/regex/regc_nfa.c:1066-1166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L1066-L1166)

## Overview
Moves all outgoing arcs from one state to another state, transferring ownership and removing arcs from the source state.

## Definition

```c
static void
moveouts(struct nfa *nfa,
		 struct state *oldState,
		 struct state *newState)
```
## Detailed Description
The `moveouts` function transfers all outgoing arcs from an old state to a new state. It employs different strategies based on the number of arcs involved: for small numbers of arcs, it processes them individually; for larger numbers, it uses a sort-merge approach to efficiently handle duplicates. When the new state has no existing outgoing arcs, it can directly transfer arcs without deduplication. The function ensures that duplicate arcs are eliminated during the transfer process, and after completion, the old state will have no outgoing arcs remaining.

The function includes optimization paths for different scenarios and performs interrupt checking for long-running operations.

## Parameters / Member Variables
- `nfa`: Pointer to the NFA structure being modified
- `oldState`: Source state from which outgoing arcs will be moved (must be different from newState)
- `newState`: Destination state that will receive the outgoing arcs

## Dependencies
- Functions called/Symbols referenced:
  - [createarc](../c/createarc.md)
  - [freearc](../f/freearc.md)
  - BULK_ARC_OP_USE_SORT
  - [cparc](../c/cparc.md)
  - INTERRUPT
  - [sortouts](../s/sortouts.md)
  - NISERR
  - [sortouts_cmp](../s/sortouts_cmp.md)
  - [changearcsource](../c/changearcsource.md)
  - NOTREACHED
- Called from (representative examples):
  - [push](../p/push.md)
  - [fixempties](../f/fixempties.md)
  - ARCV
  - REDUCE

## Notes and Other Information
- The function ensures oldState and newState are different states (assertion check)
- After completion, oldState will have zero outgoing arcs
- Uses different algorithms based on arc count for optimal performance
- Includes interrupt checking to allow cancellation during long operations
- Part of the NFA manipulation utilities for regex compilation
- Located in src/backend/regex/regc_nfa.c:1066-1166

## Simplified Source

```c
static void
moveouts(struct nfa *nfa, struct state *oldState, struct state *newState)
{
    assert(oldState != newState);

    if (newState->nouts == 0) {
        // Simple case: no duplicates to check
        struct arc *a;
        while ((a = oldState->outs) != NULL) {
            createarc(nfa, a->type, a->co, newState, a->to);
            freearc(nfa, a);
        }
    }
    else if (!BULK_ARC_OP_USE_SORT(oldState->nouts, newState->nouts)) {
        // Few arcs: process individually with duplicate checking
        struct arc *a;
        while ((a = oldState->outs) != NULL) {
            cparc(nfa, a, newState, a->to);
            freearc(nfa, a);
        }
    }
    else {
        // Many arcs: use efficient sort-merge approach
        INTERRUPT(nfa->v->re);

        sortouts(nfa, oldState);
        sortouts(nfa, newState);
        if (NISERR()) return;

        struct arc *oa = oldState->outs;
        struct arc *na = newState->outs;

        // Merge sorted arc lists, eliminating duplicates
        while (oa != NULL && na != NULL) {
            struct arc *a = oa;
            switch (sortouts_cmp(&oa, &na)) {
                case -1:  // unique arc, move it
                    oa = oa->outchain;
                    changearcsource(a, newState);
                    break;
                case 0:   // duplicate, skip it
                    oa = oa->outchain;
                    na = na->outchain;
                    freearc(nfa, a);
                    break;
                case +1:  // advance newState list
                    na = na->outchain;
                    break;
            }
        }

        // Move remaining unique arcs
        while (oa != NULL) {
            struct arc *a = oa;
            oa = oa->outchain;
            changearcsource(a, newState);
        }
    }

    assert(oldState->nouts == 0 && oldState->outs == NULL);
}
```