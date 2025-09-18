# deltraverse

## Location
src/backend/regex/regc_nfa.c: 1304 - 1354

## Overview
Recursively destroys all outgoing arcs of a state and unreachable states in a depth-first manner.

## Definition


## Detailed Description
The `deltraverse` function is the recursive core of the `delsub` operation, responsible for systematically destroying all outgoing arcs from a given state and cleaning up any states that become unreachable as a result. It employs a depth-first traversal strategy, recursively processing each destination state before freeing the arc that leads to it. The function includes protection against stack overflow through depth checking and uses a temporary marking system to track states currently being processed to avoid infinite loops.

When a state becomes unreachable (no incoming arcs) and is not currently being processed, the function automatically frees that state. The function ensures that the traversal respects the leftend boundary and maintains NFA structural integrity throughout the deletion process.

## Parameters / Member Variables
- `nfa`: Pointer to the NFA structure being modified
- `leftend`: Boundary state that should not be deleted (represents left endpoint of subexpression)
- `s`: Current state whose outgoing arcs are to be destroyed

## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP
  - NERR
  - REG_ETOOBIG
  - deltraverse (recursive call)
  - NISERR
  - freearc
  - freestate
  - FREESTATE
- Called from (representative examples):
  - delsub
  - deltraverse (recursive)

## Notes and Other Information
- Recursive function with stack overflow protection
- Uses temporary state marking (tmp field) to track processing status and prevent infinite loops
- Automatically cleans up unreachable states that have no incoming arcs
- Maintains structural integrity by preserving the leftend state and reachable states
- Part of the NFA cleanup and deletion subsystem
- Critical for memory management during regex compilation and optimization
- Located in src/backend/regex/regc_nfa.c:1304-1354