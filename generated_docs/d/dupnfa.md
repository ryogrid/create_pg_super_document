# dupnfa

## Location
src/backend/regex/regc_nfa.c: 1355 - 1378

## Overview
Duplicates a sub-NFA (non-deterministic finite automaton) between specified start and stop states, creating a copy that is connected from a source state to a destination state.

## Definition


## Detailed Description
This function performs a recursive traversal to duplicate a portion of an NFA between two states (start and stop). It uses the tmp pointer in states to both mark already-seen states and point to their duplicates during the duplication process. The duplicated sub-NFA is connected from the 'from' state to the 'to' state. If the start and stop states are the same, it simply creates an empty arc between from and to.

## Parameters / Member Variables
- : The NFA structure containing the automaton being modified
- : The starting state of the sub-NFA to be duplicated
- : The ending state of the sub-NFA to be duplicated  
- : The state from which the duplicated sub-NFA should be connected
- : The state to which the duplicated sub-NFA should be connected

## Dependencies
- Functions called/Symbols referenced:
  - [newarc](../n/newarc.md)
  - EMPTY
  - [duptraverse](duptraverse.md)
  - [cleartraverse](../c/cleartraverse.md)
- Called from (representative examples):
  - ARCV (multiple locations in regcomp.c)
  - REDUCE (multiple locations in regcomp.c)
  - [nfanode](../n/nfanode.md)

## Notes and Other Information
The function uses a clever design where the tmp pointer in state structures serves dual purposes: marking visited states during traversal and pointing to their duplicates. After duplication is complete, the tmp pointers are cleared by calling cleartraverse to maintain the integrity of the NFA structure.