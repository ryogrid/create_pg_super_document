# getvacant

## Location
src/backend/regex/rege_dfa.c: 973 - 1043

## Overview
A static function that obtains a vacant state set for reuse in the DFA regex engine, clearing its inbound and outbound arcs while preserving the caller's responsibility for clearing internal state.

## Definition


## Detailed Description
The  function is a core component of PostgreSQL's regex DFA (Deterministic Finite Automaton) engine that manages state set recycling. It obtains a vacant state set by calling  and then performs comprehensive cleanup of the state set's arc connections. The function meticulously clears both incoming and outgoing arcs to prepare the state set for reuse, while also handling special cases for post-match states and no-progress states by updating the DFA's tracking pointers.

The cleanup process involves two main phases: first, it clears all incoming arcs by traversing the incoming arc chain and removing corresponding outgoing arc references from source states. Second, it removes the state set from the incoming arc chains of all states that it points to via outgoing arcs. This bidirectional cleanup ensures no dangling references remain in the arc structure.

## Parameters / Member Variables
- : Pointer to the vars structure containing regex execution variables and context
- : Pointer to the DFA structure representing the finite automaton
- : Current character pointer in the input string being matched  
- : Pointer to the start of the input string being processed

## Dependencies
- Functions called/Symbols referenced:
  - pickss (to obtain a candidate state set)
  - FDEBUG (debugging macro for tracing operations)
  - assert (assertion macro for runtime checks)
- Called from (representative examples):
  - initialize (during DFA initialization)
  - miss (when handling cache misses)
  - LOFF (in regex execution engine)

## Notes and Other Information
The function includes important state tracking for optimization: if the cleared state set was a POSTSTATE (success state), it updates  to remember the furthest successful match position. Similarly, for NOPROGRESS states, it updates . This tracking helps the regex engine make decisions about backtracking and match validation. The function assumes the caller will handle initialization of the state set's internal state representation after the arc cleanup is complete.