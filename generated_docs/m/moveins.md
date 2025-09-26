# moveins

## Location
[src/backend/regex/regc_nfa.c:778-881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L778-L881)

## Overview
Moves all incoming arcs from one NFA state to another state, with intelligent duplicate suppression and performance optimizations based on the number of arcs involved.

## Definition

```c
static void
moveins(struct nfa *nfa,
		struct state *oldState,
		struct state *newState)
```
## Detailed Description
This function transfers all incoming arcs from oldState to newState with three different strategies depending on the circumstances:

1. **No deduplication needed**: When newState has no existing incoming arcs, arcs are simply moved without duplicate checking using createarc/freearc.

2. **Small number of arcs**: Uses retail duplicate checking by calling cparc() for each arc, which handles deduplication automatically.

3. **Large number of arcs**: Uses a sort-merge approach for efficiency:
   - Sorts both states' incoming arc lists using sortins()
   - Merges the lists while detecting duplicates
   - Uses changearctarget() to move unique arcs in-place
   - Frees duplicate arcs

The function includes optimization logic via BULK_ARC_OP_USE_SORT() to determine when the sort-merge approach is more efficient than individual processing.

## Parameters / Member Variables
- : Pointer to the NFA structure containing the states
- : Source state whose incoming arcs will be moved
- : Destination state that will receive the arcs

## Dependencies
- Functions called/Symbols referenced:
  - [createarc](../c/createarc.md) (creates new arc)
  - [freearc](../f/freearc.md) (frees an arc)
  - [cparc](../c/cparc.md) (copies arc with duplicate suppression)
  - [sortins](../s/sortins.md) (sorts incoming arcs)
  - [changearctarget](../c/changearctarget.md) (changes arc target state)
  - BULK_ARC_OP_USE_SORT (macro to determine sort strategy)
  - INTERRUPT (cancellation check macro)
  - NISERR (error checking macro)
  - [sortins_cmp](../s/sortins_cmp.md) (comparison function for sorting)
  - NOTREACHED (assertion macro)
  - [arc](../a/arc.md) (struct type)
- Called from (representative examples):
  - [pull](../p/pull.md) (src/backend/regex/regc_nfa.c:1801)
  - [fixempties](../f/fixempties.md) (src/backend/regex/regc_nfa.c:2106)
  - [parsebranch](../p/parsebranch.md) (src/backend/regex/regcomp.c:806)
  - ARCV (src/backend/regex/regcomp.c:1204)
  - REDUCE (src/backend/regex/regcomp.c:1619, 1640)

## Notes and Other Information
- This is a static function local to the regc_nfa.c file
- The function ensures oldState has no remaining incoming arcs after completion
- Performance is optimized through different strategies based on arc count
- The sort-merge approach bypasses newarc() for efficiency but includes cancellation checks
- Duplicate detection is crucial for maintaining NFA correctness
- The function handles three distinct code paths to balance performance with correctness
- After completion, oldState->nins should be 0 and oldState->ins should be NULL