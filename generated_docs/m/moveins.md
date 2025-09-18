# moveins

## Location
src/backend/regex/regc_nfa.c: 778 - 881

## Overview
Moves all incoming arcs from one NFA state to another state, with intelligent duplicate suppression and performance optimizations based on the number of arcs involved.

## Definition


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
  - createarc (creates new arc)
  - freearc (frees an arc)
  - cparc (copies arc with duplicate suppression)
  - sortins (sorts incoming arcs)
  - changearctarget (changes arc target state)
  - BULK_ARC_OP_USE_SORT (macro to determine sort strategy)
  - INTERRUPT (cancellation check macro)
  - NISERR (error checking macro)
  - sortins_cmp (comparison function for sorting)
  - NOTREACHED (assertion macro)
  - arc (struct type)
- Called from (representative examples):
  - pull (src/backend/regex/regc_nfa.c:1801)
  - fixempties (src/backend/regex/regc_nfa.c:2106)
  - parsebranch (src/backend/regex/regcomp.c:806)
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