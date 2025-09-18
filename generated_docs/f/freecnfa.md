# freecnfa

## Location
src/backend/regex/regc_nfa.c: 3633 - 3645

## Overview
A static function that properly deallocates memory for a compacted NFA (Non-deterministic Finite Automaton) structure used in PostgreSQL's regular expression processing.

## Definition


## Detailed Description
The  function is responsible for cleaning up and deallocating all memory associated with a compacted NFA structure. It ensures proper memory management by freeing the individual components of the NFA (state flags, states, and arcs) and then zeroing out the structure to mark it as empty. This function is part of PostgreSQL's regex compilation subsystem and is called when a compiled NFA is no longer needed.

The function includes an assertion to verify that the NFA is not already empty before attempting to free it, providing a safety check against double-free scenarios.

## Parameters / Member Variables
- : Pointer to the compacted NFA structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - NULLCNFA (macro to check if NFA is empty)
  - FREE (macro for memory deallocation)
  - ZAPCNFA (macro to zero out the NFA structure)
- Called from (representative examples):
  - REPLACEARC (src/backend/regex/regcomp.c:233)
  - [freesrnode](freesrnode.md) (src/backend/regex/regcomp.c:2194)
  - [freelacons](freelacons.md) (src/backend/regex/regcomp.c:2439)
  - [rfree](../r/rfree.md) (src/backend/regex/regcomp.c:2467)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the regc_nfa.c source file
- The function follows a defensive programming pattern by asserting the NFA is not already empty
- Memory cleanup follows the proper order: first free the dynamic arrays (stflags, states, arcs), then zero the structure
- Part of PostgreSQL's regex engine implementation for pattern matching and text processing