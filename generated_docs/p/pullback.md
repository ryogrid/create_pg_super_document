# pullback

## Location
[src/backend/regex/regc_nfa.c:1640-1719](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L1640-L1719)

## Overview
Eliminates constraint arcs (^ and BEHIND) by pulling them backward through the NFA structure, ultimately converting them to PLAIN arcs with special boundary colors.

## Definition

```c
struct nfa *nfa,
		 FILE *f)				/* for debug output;
```
## Detailed Description
This function implements a critical optimization step that eliminates constraint arcs from the NFA by pulling them backward toward the start state. The process works iteratively:

1. **Iterative Processing**: Scans all states and their outgoing arcs repeatedly until no more progress can be made
2. **Constraint Identification**: Looks for '^' (beginning-of-line) and BEHIND (lookbehind assertion) arcs  
3. **Backward Pulling**: Uses the  function to move these constraints backward through the NFA
4. **Intermediate State Management**: Tracks and cleans up temporary intermediate states created during the pulling process
5. **State Cleanup**: Removes states that become useless (no inputs or outputs) after constraint removal
6. **Final Conversion**: Converts any remaining '^' constraints at the start state to PLAIN arcs using special BOS/BOL colors

The function ensures that constraint arcs are either eliminated entirely or moved to positions where they can be converted to regular color-based arcs that the executor can handle.

## Parameters / Member Variables
- : Pointer to the NFA structure being optimized
- : File pointer for debug output; NULL if no debug output desired

## Dependencies
- Functions called/Symbols referenced:
  - : Core function that performs the backward pulling of individual constraints
  - : Removes useless states from the NFA
  - : Creates new arcs in the NFA
  - : Deallocates arc structures
  - : Debug output function (when debug enabled)
  - : Error checking macro
  - : Constant representing lookbehind assertion arcs
  - : Constant representing regular character-matching arcs
- Called from (representative examples):
  -  (src/backend/regex/regc_nfa.c:1622)

## Notes and Other Information
- This is a static function, only visible within the regc_nfa.c compilation unit
- Part of the NFA optimization pipeline that prepares regex structures for execution
- Works in conjunction with  to handle constraint elimination
- The function uses a progress-driven loop to ensure all possible constraint eliminations are performed
- Successfully pulled '^' constraints are converted to PLAIN arcs using  colors
- Critical for ensuring the final NFA contains only arc types the executor can process
- Handles cleanup of temporary intermediate states created during the pulling process

## Simplified Source
```c
static void pullback(struct nfa *nfa, FILE *f) {
    struct state *s, *nexts;
    struct arc *a, *nexta;
    struct state *intermediates;
    int progress;

    // Pull constraints backward until no more progress
    do {
        progress = 0;
        for (s = nfa->states; s != NULL && !NISERR(); s = nexts) {
            nexts = s->next;
            intermediates = NULL;

            // Check each outgoing arc for constraints to pull
            for (a = s->outs; a != NULL && !NISERR(); a = nexta) {
                nexta = a->outchain;
                if (a->type == '^' || a->type == BEHIND) {
                    if (pull(nfa, a, &intermediates))
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

    // Convert remaining '^' constraints at start to PLAIN arcs
    for (a = nfa->pre->outs; a != NULL; a = nexta) {
        nexta = a->outchain;
        if (a->type == '^') {
            newarc(nfa, PLAIN, nfa->bos[a->co], a->from, a->to);
            freearc(nfa, a);
        }
    }
}
```