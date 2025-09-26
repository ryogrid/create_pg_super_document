# findconstraintloop

## Location
src/backend/regex/regc_nfa.c: 2469 - 2557

## Overview
Recursively searches for loops of constraint arcs in the NFA and breaks them when found to prevent infinite loops during regex compilation.

## Definition


## Detailed Description
This function implements a depth-first search algorithm to detect constraint loops in the NFA starting from a given state. It uses the temporary field (tmp) of states to track the search path and identify cycles. When a loop is detected, it calls breakconstraintloop() to eliminate the loop and returns 1 to indicate success.

The algorithm works by:
1. **Stack overflow protection**: Checks recursion depth to prevent stack overflow
2. **Cycle detection**: Uses state tmp fields to detect when the search revisits a state in the current path
3. **Loop breaking**: Calls breakconstraintloop() when a cycle is found
4. **Memoization**: Marks states that don't lead to loops with tmp == s to avoid redundant searches

The function employs an optimization where states proven not to be part of any constraint loop are marked with s->tmp == s, allowing subsequent searches to skip them efficiently.

## Parameters / Member Variables
- : Pointer to the NFA structure being analyzed
- : Starting state for the constraint loop search

## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP (macro to check recursion depth)
  - NERR (error reporting macro)
  - REG_ETOOBIG (error code for overly complex regex)
  - breakconstraintloop (function to break detected loops)
  - isconstraintarc (checks if an arc is a constraint arc)
  - findconstraintloop (recursive self-call)
- Called from (representative examples):
  - fixconstraintloops (main constraint loop fixing function)
  - findconstraintloop (recursive self-calls)

## Notes and Other Information
- This is a recursive function with potential for deep recursion in complex NFAs
- Uses state tmp fields both for cycle detection and memoization of negative results
- The found loop doesn't necessarily include the starting state - any reachable loop suffices
- Single-state loops are assumed to be already eliminated before this function is called
- Maximum recursion depth is bounded by the longest chain of constraint arcs in the NFA
- Returns 1 if a loop was found and broken, 0 if no loop exists from the starting state
- Tmp fields are guaranteed to be NULL on success return due to breakconstraintloop cleanup