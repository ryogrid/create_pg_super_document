# pull

## Location
[src/backend/regex/regc_nfa.c:1720-1810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L1720-L1810)

## Overview
Pulls a single back constraint arc (^ or BEHIND) backward past its source state, handling the complex logic of constraint propagation through the NFA structure.

## Definition


## Detailed Description
This function implements the core logic for moving constraint arcs backward in the NFA. The process involves several complex steps:

1. **Validation**: Ensures the constraint can be pulled (not from start state, state is reachable)
2. **State Cloning**: If the source state has multiple outgoing arcs, it clones the state to isolate the constraint
3. **Constraint Propagation**: For each incoming arc to the source state, determines how the constraint interacts using 
4. **Action Based on Compatibility**:
   - **INCOMPATIBLE**: Destroys the incompatible arc
   - **SATISFIED**: No action needed (constraint already satisfied)
   - **COMPATIBLE**: Creates intermediate states to maintain both constraint and arc
   - **REPLACEARC**: Replaces the arc's color with the constraint's color

5. **Intermediate State Management**: Reuses existing intermediate states when possible to avoid duplication
6. **Cleanup**: Moves remaining arcs and frees the constraint arc

The function preserves existing states and arcs (except the target constraint) to maintain loop safety in the calling  function.

## Parameters / Member Variables
- : Pointer to the NFA structure being modified
- : The constraint arc to be pulled backward  
- : Pointer to linked list of intermediate states (chained via tmp fields)

**Return Value**: Returns 1 if successful, 0 if no action was taken (e.g., at start state)

## Dependencies
- Functions called/Symbols referenced:
  - : Creates new NFA states
  - : Duplicates incoming arcs to a state
  - : Copies an arc between specified states
  - : Deallocates arc structures
  - : Determines how constraint interacts with other arcs
  - : Moves incoming arcs from one state to another
  - : Creates new arcs
  - : Error checking macro
  - , , , : Constants from combine() results
  - : Assertion constant for impossible cases
- Called from (representative examples):
  -  (src/backend/regex/regc_nfa.c:1662)

## Notes and Other Information
- This is a static function, only visible within the regc_nfa.c compilation unit
- Critical component of constraint elimination in regex optimization
- Designed to be safe for use in loops by not deleting pre-existing states
- Uses intermediate state caching to avoid creating duplicate states for the same predecessor/successor combinations
- The function handles self-loops by asserting  (these should be eliminated earlier)
- Leaves cleanup of useless states to the calling  function
- Part of PostgreSQL's regex engine constraint elimination system