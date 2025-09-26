# clonesuccessorstates

## Location
src/backend/regex/regc_nfa.c: 2704 - 2937

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
- : Pointer to the NFA structure being modified
- : Source state to be cloned
- : Target clone state to copy outarcs into
- : Original predecessor state for context
- : Reference constraint arc that was traversed to reach successors (may be NULL)
- : Current donemap for tracking visited states (NULL for new clone states)
- : Parent clone state's donemap for inheritance
- : Size of donemaps (original NFA state count before cloning)

## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP (recursion depth checking macro)
  - NERR (error reporting macro)
  - REG_ETOOBIG, REG_ESPACE (error codes)
  - MALLOC, FREE (memory management)
  - NISERR (error checking macro)
  - isconstraintarc (constraint arc identification)
  - hasconstraintout (checks if state has constraint outarcs)
  - dropstate (removes a state from NFA)
  - cparc (copies an arc between states)
  - newstate (creates new NFA state)
  - clonesuccessorstates (recursive self-calls)
- Called from (representative examples):
  - breakconstraintloop (main entry point for loop breaking)
  - clonesuccessorstates (recursive self-calls)

## Notes and Other Information
- Uses tmp fields in clone states to track their source states during processing
- Implements sophisticated constraint satisfaction checking to enable state merging
- Handles complex scenarios including multiple paths to the same state
- Creates strict tree structures with exactly one predecessor per state
- Manages memory carefully with proper donemap allocation and deallocation
- Prevents infinite recursion through both stack depth checking and visited state tracking
- Non-constraint outarcs and states without constraint outarcs are linked as-is rather than cloned
- The donemap inheritance mechanism prevents revisiting states being processed at outer recursion levels