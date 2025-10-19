# checkmatchall

## Location
[src/backend/regex/regc_nfa.c:3097-3276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L3097-L3276)

## Overview
The checkmatchall function analyzes an NFA (Nondeterministic Finite Automaton) to determine if it represents a simple string length test, optimizing regex matching for patterns that only care about string length.

## Definition

```c
static void
checkmatchall(struct nfa *nfa)
```
## Detailed Description
This function performs a sophisticated analysis to detect if an NFA represents a 'matchall' pattern - essentially a regex that only tests string length without caring about specific character content (like  which matches any 5-10 character string). When such a pattern is detected, it sets optimization flags and length bounds that allow the regex engine to use faster matching algorithms.

The function performs several validation steps:
1. Checks if the NFA has too many states (> DUPINF * 2) and aborts if so
2. Verifies that all arcs are PLAIN RAINBOW arcs (matching any character) or valid pseudocolor arcs (BOS/BOL/EOS/EOL)
3. Validates that pseudocolor arcs properly replicate RAINBOW arcs at pre/post states
4. Uses recursive analysis to find all possible path lengths through the NFA
5. Ensures path lengths form a consecutive range (no gaps)

If all conditions are met, the function sets nfa->minmatchall, nfa->maxmatchall, and the MATCHALL flag, enabling significant optimization in the regex execution engine.

## Parameters / Member Variables
- `*nfa`: Pointer to the NFA structure to analyze for matchall optimization
## Dependencies
- Functions called/Symbols referenced:
  - DUPINF (maximum duplication count constant)
  - PLAIN, RAINBOW, PSEUDO (arc type/color constants)
  - [check_out_colors_match](check_out_colors_match.md) (validates outgoing arc color consistency)
  - [check_in_colors_match](check_in_colors_match.md) (validates incoming arc color consistency)
  - MALLOC (memory allocation)
  - [checkmatchall_recurse](checkmatchall_recurse.md) (recursive path analysis)
  - MATCHALL (optimization flag constant)
  - FREE (memory deallocation)
- Called from (representative examples):
  - analyze (src/backend/regex/regc_nfa.c:3064)
  - REPLACEARC macro (src/backend/regex/regcomp.c:225)

## Notes and Other Information
- This is a static function, only accessible within the regc_nfa.c file
- The function implements a complex optimization for regex patterns that only test string length
- Uses dynamic memory allocation for path analysis arrays with proper cleanup
- Handles edge cases like multi-state loops and paths exceeding DUPINF length
- Critical for performance optimization in PostgreSQL's regex engine when dealing with length-only patterns
- The optimization can significantly speed up matching for patterns like  or  with length constraints
- Pseudocolor arcs (BOS/BOL/EOS/EOL) are carefully validated to ensure they don't introduce character-specific constraints

## Simplified Source

```c
static void
checkmatchall(struct nfa *nfa)
{
    bool **haspaths;
    struct state *s;
    int i;

    // Skip analysis if too many states
    if (nfa->nstates > DUPINF * 2)
        return;

    // Verify all arcs are RAINBOW or valid pseudocolors
    for (s = nfa->states; s != NULL; s = s->next) {
        struct arc *a;
        for (a = s->outs; a != NULL; a = a->outchain) {
            if (a->type != PLAIN)
                return;  // LACONs make it non-matchall

            if (a->co != RAINBOW) {
                // Check if it's a valid pseudocolor arc
                if (!(nfa->cm->cd[a->co].flags & PSEUDO) ||
                    !((s == nfa->pre && (a->co == nfa->bos[0] || a->co == nfa->bos[1])) ||
                      (a->to == nfa->post && (a->co == nfa->eos[0] || a->co == nfa->eos[1]))))
                    return;
            }
        }
    }

    // Verify pseudocolor arcs match RAINBOW arcs
    if (!check_out_colors_match(nfa->pre, RAINBOW, nfa->bos[0]) ||
        !check_out_colors_match(nfa->pre, RAINBOW, nfa->bos[1]) ||
        !check_in_colors_match(nfa->post, RAINBOW, nfa->eos[0]) ||
        !check_in_colors_match(nfa->post, RAINBOW, nfa->eos[1]))
        return;

    // Allocate path analysis arrays
    haspaths = (bool **) MALLOC(nfa->nstates * sizeof(bool *));
    if (haspaths == NULL)
        return;
    memset(haspaths, 0, nfa->nstates * sizeof(bool *));

    // Analyze all paths for consecutive length ranges
    if (checkmatchall_recurse(nfa, nfa->pre, haspaths)) {
        bool *haspath = haspaths[nfa->pre->no];
        int minmatch, maxmatch, morematch;

        // Find min and max of consecutive path lengths
        for (minmatch = 0; minmatch <= DUPINF + 1; minmatch++) {
            if (haspath[minmatch])
                break;
        }
        for (maxmatch = minmatch; maxmatch < DUPINF + 1; maxmatch++) {
            if (!haspath[maxmatch + 1])
                break;
        }

        // Verify no gaps in path lengths
        for (morematch = maxmatch + 1; morematch <= DUPINF + 1; morematch++) {
            if (haspath[morematch]) {
                haspath = NULL;  // fail - nonconsecutive lengths
                break;
            }
        }

        // Set optimization flags if valid
        if (haspath != NULL) {
            nfa->minmatchall = minmatch - 1;
            nfa->maxmatchall = maxmatch - 1;
            nfa->flags |= MATCHALL;
        }
    }

    // Clean up allocated memory
    for (i = 0; i < nfa->nstates; i++) {
        if (haspaths[i] != NULL)
            FREE(haspaths[i]);
    }
    FREE(haspaths);
}
```