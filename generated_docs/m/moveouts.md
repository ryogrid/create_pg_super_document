# moveouts

## Location
src/backend/regex/regc_nfa.c: 1066 - 1166

## Overview
Moves all outgoing arcs from one state to another state, transferring ownership and removing arcs from the source state.

## Definition


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
  - push
  - fixempties
  - ARCV
  - REDUCE

## Notes and Other Information
- The function ensures oldState and newState are different states (assertion check)
- After completion, oldState will have zero outgoing arcs
- Uses different algorithms based on arc count for optimal performance
- Includes interrupt checking to allow cancellation during long operations
- Part of the NFA manipulation utilities for regex compilation
- Located in src/backend/regex/regc_nfa.c:1066-1166