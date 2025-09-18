# dumparc

## Location
src/backend/regex/regc_nfa.c: 3753 - 3822

## Overview
A debugging function that outputs detailed information about a single NFA arc in human-readable format, including arc type, color, source/destination states, and chain integrity validation.

## Definition


## Detailed Description
The  function provides a comprehensive textual representation of an individual NFA arc for debugging purposes. It displays different arc types using specific notation: PLAIN arcs with square brackets, AHEAD/BEHIND lookaround assertions with angle brackets, LACON constraints with colons, anchor assertions with their symbols, and special cases like EMPTY and CANTMATCH arcs. The function also validates arc chain integrity by checking that the arc appears in both the source state's outgoing chain and the destination state's incoming chain.

The function handles special cases like RAINBOW (match-all) colors and provides diagnostic output when arc chain inconsistencies are detected, making it an essential tool for debugging NFA structure during regex compilation.

## Parameters / Member Variables
- : Pointer to the arc structure to be dumped
- : Pointer to the source state (used for validation)
- : File stream where the arc information will be written

## Dependencies
- Functions called/Symbols referenced:
  - PLAIN (arc type for normal character transitions)
  - AHEAD (arc type for positive lookahead assertions)
  - BEHIND (arc type for positive lookbehind assertions)
  - LACON (arc type for lookaround constraints)
  - EMPTY (arc type for epsilon transitions)
  - CANTMATCH (arc type for impossible matches)
  - RAINBOW (special color value for match-all)
  - arc (struct type for representing transitions)
- Called from (representative examples):
  - dumparcs (src/backend/regex/regc_nfa.c:3735)
  - REPLACEARC (src/backend/regex/regcomp.c:239)

## Notes and Other Information
- Only available in debug builds (part of REG_DEBUG conditional compilation)
- Uses distinctive notation for different arc types:  for PLAIN,  for AHEAD,  for BEHIND,  for LACON
- Handles RAINBOW color specially with  notation for match-all transitions
- Performs chain integrity validation to detect corrupted arc structures
- Shows  markers when arcs are missing from expected chains
- Displays  when arc source doesn't match expected state
- Handles NULL destination states gracefully
- Essential for understanding NFA transition structure during regex compilation and optimization
- Part of PostgreSQL's regex engine debugging infrastructure for analyzing state machine connectivity