# pushfwd

## Location
[src/backend/regex/regc_nfa.c:1811-1890](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L1811-L1890)

## Overview
Eliminates forward constraint arcs ($ and AHEAD) by pushing them forward through the NFA structure, ultimately converting them to PLAIN arcs with special boundary colors.

## Definition

```c
struct nfa *nfa,
		FILE *f)				/* for debug output;
```
## Detailed Description
This function is the forward counterpart to , handling the elimination of forward-looking constraint arcs. The process works iteratively:

1. **Iterative Processing**: Scans all states and their incoming arcs repeatedly until no more progress can be made
2. **Constraint Identification**: Looks for '$' (end-of-line) and AHEAD (lookahead assertion) arcs
3. **Forward Pushing**: Uses the  function to move these constraints forward through the NFA
4. **Intermediate State Management**: Tracks and cleans up temporary intermediate states created during the pushing process
5. **State Cleanup**: Removes states that become useless (no inputs or outputs) after constraint removal
6. **Final Conversion**: Converts any remaining '$' constraints at the post state to PLAIN arcs using special EOS/EOL colors

The function works in tandem with  to ensure that all constraint arcs are either eliminated entirely or moved to positions where they can be converted to regular color-based arcs that the executor can handle.

## Parameters / Member Variables
- : Pointer to the NFA structure being optimized
- : File pointer for debug output; NULL if no debug output desired

## Dependencies
- Functions called/Symbols referenced:
  - : Core function that performs the forward pushing of individual constraints
  - : Removes useless states from the NFA
  - : Creates new arcs in the NFA
  - : Deallocates arc structures
  - : Debug output function (when debug enabled)
  - : Error checking macro
  - : Constant representing lookahead assertion arcs
  - : Constant representing regular character-matching arcs
- Called from (representative examples):
  -  (src/backend/regex/regc_nfa.c:1623)

## Notes and Other Information
- This is a static function, only visible within the regc_nfa.c compilation unit
- Part of the NFA optimization pipeline that prepares regex structures for execution
- Works in conjunction with  to handle complete constraint elimination
- The function uses a progress-driven loop to ensure all possible constraint eliminations are performed
- Successfully pushed '$' constraints are converted to PLAIN arcs using  colors
- Critical for ensuring the final NFA contains only arc types the executor can process
- Handles cleanup of temporary intermediate states created during the pushing process
- Processes incoming arcs () rather than outgoing arcs, complementing 's approach

## Simplified Source
```c
static void pushfwd(struct nfa *nfa, FILE *f) {
    struct state *s, *nexts;
    struct arc *a, *nexta;
    struct state *intermediates;
    int progress;

    // Push constraints forward until no more progress
    do {
        progress = 0;
        for (s = nfa->states; s != NULL && !NISERR(); s = nexts) {
            nexts = s->next;
            intermediates = NULL;

            // Check each incoming arc for constraints to push
            for (a = s->ins; a != NULL && !NISERR(); a = nexta) {
                nexta = a->inchain;
                if (a->type == '$' || a->type == AHEAD) {
                    if (push(nfa, a, &intermediates))
                        progress = 1;
                }
            }

            // Clean up intermediate states
            while (intermediates != NULL) {
                struct state *ns = intermediates->tmp;
                intermediates->tmp = NULL;
                intermediates = ns;
            }

            // Remove useless states
            if ((s->nins == 0 || s->nouts == 0) && !s->flag)
                dropstate(nfa, s);
        }
    } while (progress && !NISERR());

    // Convert remaining '$' constraints at post to PLAIN arcs
    for (a = nfa->post->ins; a != NULL; a = nexta) {
        nexta = a->inchain;
        if (a->type == '$') {
            newarc(nfa, PLAIN, nfa->eos[a->co], a->from, a->to);
            freearc(nfa, a);
        }
    }
}
```