# clonesuccessorstates

## Location
[src/backend/regex/regc_nfa.c:2704-2937](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L2704-L2937)

## Overview
Recursively creates a tree structure of cloned successor states while handling constraint arcs to break loops without losing regex functionality.

## Definition

```c
static void
clonesuccessorstates(struct nfa *nfa,
					 struct state *ssource,
					 struct state *sclone,
					 struct state *spredecessor,
					 struct arc *refarc,
					 char *curdonemap,
					 char *outerdonemap,
					 int nstates)
```
## Detailed Description
This function implements the core cloning logic for breaking constraint loops by building a tree of successor states. It intelligently merges equivalent states and avoids infinite recursion through sophisticated state tracking mechanisms.

The algorithm operates in two phases:
1. **Arc cloning phase**: Processes all outarcs from the source state, creating clone states as needed and applying merging optimizations
2. **Recursive processing phase**: Recursively processes child clone states to build the complete successor tree

Key optimizations include:
- **State merging**: When constraints are already satisfied, merges successor states into the current clone rather than creating new states
- **Donemap tracking**: Uses boolean arrays to track visited states and prevent infinite recursion
- **Constraint analysis**: Examines the path from root to current state to determine which constraints are already validated
- **Deduplication**: Ensures only one clone state per source state even with multiple incoming arcs

## Parameters / Member Variables
- `*nfa`: Pointer to the NFA structure being modified
- `*ssource`: Source state to be cloned
- `*sclone`: Target clone state to copy outarcs into
- `*spredecessor`: Original predecessor state for context
- `*refarc`: Reference constraint arc that was traversed to reach successors (may be NULL)
- `*curdonemap`: Current donemap for tracking visited states (NULL for new clone states)
- `*outerdonemap`: Parent clone state's donemap for inheritance
- `nstates`: Size of donemaps (original NFA state count before cloning)
## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP (recursion depth checking macro)
  - NERR (error reporting macro)
  - REG_ETOOBIG, REG_ESPACE (error codes)
  - MALLOC, FREE (memory management)
  - NISERR (error checking macro)
  - [isconstraintarc](../i/isconstraintarc.md) (constraint arc identification)
  - [hasconstraintout](../h/hasconstraintout.md) (checks if state has constraint outarcs)
  - [dropstate](../d/dropstate.md) (removes a state from NFA)
  - [cparc](cparc.md) (copies an arc between states)
  - [newstate](../n/newstate.md) (creates new NFA state)
  - [clonesuccessorstates](clonesuccessorstates.md) (recursive self-calls)
- Called from (representative examples):
  - [breakconstraintloop](../b/breakconstraintloop.md) (main entry point for loop breaking)
  - [clonesuccessorstates](clonesuccessorstates.md) (recursive self-calls)

## Notes and Other Information
- Uses tmp fields in clone states to track their source states during processing
- Implements sophisticated constraint satisfaction checking to enable state merging
- Handles complex scenarios including multiple paths to the same state
- Creates strict tree structures with exactly one predecessor per state
- Manages memory carefully with proper donemap allocation and deallocation
- Prevents infinite recursion through both stack depth checking and visited state tracking
- Non-constraint outarcs and states without constraint outarcs are linked as-is rather than cloned
- The donemap inheritance mechanism prevents revisiting states being processed at outer recursion levels

## Simplified Source

```c
static void
clonesuccessorstates(struct nfa *nfa, struct state *ssource, struct state *sclone,
                     struct state *spredecessor, struct arc *refarc,
                     char *curdonemap, char *outerdonemap, int nstates)
{
    char *donemap;
    struct arc *a;

    // Stack overflow protection
    if (STACK_TOO_DEEP(nfa->v->re)) {
        NERR(REG_ETOOBIG);
        return;
    }

    // Initialize donemap for tracking visited states
    donemap = curdonemap;
    if (donemap == NULL) {
        donemap = (char *) MALLOC(nstates * sizeof(char));
        if (donemap == NULL) {
            NERR(REG_ESPACE);
            return;
        }

        if (outerdonemap != NULL)
            memcpy(donemap, outerdonemap, nstates * sizeof(char));
        else {
            memset(donemap, 0, nstates * sizeof(char));
            donemap[spredecessor->no] = 1;  // Mark predecessor as off-limits
        }
    }

    // Mark current source state as visited
    donemap[ssource->no] = 1;

    // Phase 1: Clone all outarcs, creating child clone states as needed
    for (a = ssource->outs; a != NULL && !NISERR(); a = a->outchain) {
        struct state *sto = a->to;

        if (isconstraintarc(a) && hasconstraintout(sto)) {
            // Don't revisit already processed states
            if (donemap[sto->no] != 0)
                continue;

            // Check if we already have a clone for this destination
            struct state *prevclone = NULL;
            struct arc *a2;
            for (a2 = sclone->outs; a2 != NULL; a2 = a2->outchain) {
                if (a2->to->tmp == sto) {
                    prevclone = a2->to;
                    break;
                }
            }

            // Determine if we can merge states based on constraint satisfaction
            int canmerge = 0;
            if (refarc && a->type == refarc->type && a->co == refarc->co)
                canmerge = 1;
            else {
                struct state *s;
                for (s = sclone; s->ins; s = s->ins->from) {
                    if (s->nins == 1 && a->type == s->ins->type && a->co == s->ins->co) {
                        canmerge = 1;
                        break;
                    }
                }
            }

            if (canmerge) {
                // Merge into current clone state
                if (prevclone)
                    dropstate(nfa, prevclone);
                clonesuccessorstates(nfa, sto, sclone, spredecessor, refarc,
                                     donemap, outerdonemap, nstates);
            } else if (prevclone) {
                // Reuse existing clone
                cparc(nfa, a, sclone, prevclone);
            } else {
                // Create new clone state
                struct state *stoclone = newstate(nfa);
                if (stoclone == NULL)
                    break;
                stoclone->tmp = sto;
                cparc(nfa, a, sclone, stoclone);
            }
        } else {
            // Non-constraint arcs: copy as-is
            cparc(nfa, a, sclone, sto);
        }
    }

    // Phase 2: Recursively process child clone states
    if (curdonemap == NULL) {
        for (a = sclone->outs; a != NULL && !NISERR(); a = a->outchain) {
            struct state *stoclone = a->to;
            struct state *sto = stoclone->tmp;

            if (sto != NULL) {
                stoclone->tmp = NULL;
                clonesuccessorstates(nfa, sto, stoclone, spredecessor, refarc,
                                     NULL, donemap, nstates);
            }
        }
        FREE(donemap);
    }
}
```