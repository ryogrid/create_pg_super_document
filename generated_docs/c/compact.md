# compact

## Location
[src/backend/regex/regc_nfa.c:3514-3604](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L3514-L3604)

## Overview
Constructs the compact representation of an NFA (Non-deterministic Finite Automaton) by converting it into a CNFA (Compact NFA) structure for efficient runtime execution.

## Definition

```c
static void
compact(struct nfa *nfa,
		struct cnfa *cnfa)
```
## Detailed Description
This function transforms a regular NFA into a compact representation optimized for execution. The compact form uses arrays instead of linked lists for better cache locality and faster traversal during pattern matching. The conversion process involves:

1. **Memory allocation**: Allocates arrays for state flags, state pointers, and arc storage
2. **State mapping**: Maps NFA states to compact array indices
3. **Arc conversion**: Converts linked arc lists to contiguous arrays, handling both PLAIN and LACON (lookahead/lookbehind assertion) arc types
4. **Sorting**: Sorts arcs within each state using  for efficient searching
5. **Metadata transfer**: Copies essential NFA properties like pre/post states, BOS/EOS markers, colors, and flags
6. **No-progress marking**: Identifies states that don't advance the input position

The function handles memory allocation failures gracefully and ensures all arc arrays are properly terminated with COLORLESS endmarkers.

## Parameters / Member Variables
- `*nfa`: Source NFA structure to be converted
- `*cnfa`: Target compact NFA structure to be populated
## Dependencies
- Functions called/Symbols referenced:
  -  (error checking macro)
  - ,  (memory management)
  -  (color management)
  -  (arc sorting)
  -  (error reporting)
  - Constants: , , , , , , 
- Called from (representative examples):
  -  (at src/backend/regex/regcomp.c:2381)

## Notes and Other Information
- Critical for PostgreSQL's regex engine performance optimization
- Converts dynamic linked structures to static arrays for better cache performance
- Handles both regular arcs (PLAIN) and lookaround assertion arcs (LACON)
- LACON arcs use colors beyond the normal color range (ncolors + lacon_id)
- Each state's arc array is terminated with a COLORLESS endmarker
- No-progress states are specially marked to prevent infinite loops in matching
- Memory allocation failure results in REG_ESPACE error
- Arc sorting within states enables binary search during execution

## Simplified Source

```c
static void
compact(struct nfa *nfa, struct cnfa *cnfa)
{
    struct state *s;
    struct arc *a;
    size_t nstates, narcs;
    struct carc *ca, *first;

    // Count states and arcs needed
    nstates = 0;
    narcs = 0;
    for (s = nfa->states; s != NULL; s = s->next) {
        nstates++;
        narcs += s->nouts + 1;  // +1 for endmarker
    }

    // Allocate compact arrays
    cnfa->stflags = (char *) MALLOC(nstates * sizeof(char));
    cnfa->states = (struct carc **) MALLOC(nstates * sizeof(struct carc *));
    cnfa->arcs = (struct carc *) MALLOC(narcs * sizeof(struct carc));

    if (cnfa->stflags == NULL || cnfa->states == NULL || cnfa->arcs == NULL) {
        // Clean up partial allocation
        if (cnfa->stflags != NULL) FREE(cnfa->stflags);
        if (cnfa->states != NULL) FREE(cnfa->states);
        if (cnfa->arcs != NULL) FREE(cnfa->arcs);
        NERR(REG_ESPACE);
        return;
    }

    // Copy NFA metadata
    cnfa->nstates = nstates;
    cnfa->pre = nfa->pre->no;
    cnfa->post = nfa->post->no;
    cnfa->bos[0] = nfa->bos[0];
    cnfa->bos[1] = nfa->bos[1];
    cnfa->eos[0] = nfa->eos[0];
    cnfa->eos[1] = nfa->eos[1];
    cnfa->ncolors = maxcolor(nfa->cm) + 1;
    cnfa->flags = nfa->flags;
    cnfa->minmatchall = nfa->minmatchall;
    cnfa->maxmatchall = nfa->maxmatchall;

    // Convert states and arcs
    ca = cnfa->arcs;
    for (s = nfa->states; s != NULL; s = s->next) {
        cnfa->stflags[s->no] = 0;
        cnfa->states[s->no] = ca;
        first = ca;

        // Convert outgoing arcs
        for (a = s->outs; a != NULL; a = a->outchain) {
            switch (a->type) {
                case PLAIN:
                    ca->co = a->co;
                    ca->to = a->to->no;
                    ca++;
                    break;
                case LACON:
                    ca->co = (color) (cnfa->ncolors + a->co);
                    ca->to = a->to->no;
                    ca++;
                    cnfa->flags |= HASLACONS;
                    break;
                default:
                    NERR(REG_ASSERT);
                    return;
            }
        }

        // Sort arcs and add endmarker
        carcsort(first, ca - first);
        ca->co = COLORLESS;
        ca->to = 0;
        ca++;
    }

    // Mark no-progress states
    for (a = nfa->pre->outs; a != NULL; a = a->outchain)
        cnfa->stflags[a->to->no] = CNFA_NOPROGRESS;
    cnfa->stflags[nfa->pre->no] = CNFA_NOPROGRESS;
}
```