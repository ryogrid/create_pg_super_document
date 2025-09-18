# makesearch

## Location
src/backend/regex/regcomp.c: 621 - 716

## Overview
Transforms an NFA into a search NFA by implicitly prepending  (non-greedy match-all) to handle non-anchored pattern matching and resolving state conflicts from multiple entry points.

## Definition


## Detailed Description
The  function converts a standard NFA (Non-deterministic Finite Automaton) into a search NFA suitable for finding patterns anywhere within input text, not just at the beginning. This transformation involves two main phases:

**Phase 1: Adding Implicit Prefix**
- Checks if the pattern is anchored by examining arcs from the pre-state for beginning-of-string markers
- For unanchored patterns, adds implicit  (match any character zero or more times) from pre-state to itself
- Also adds  and  loops to handle different anchoring scenarios
- Updates MATCHALL flag behavior to reflect infinite maximum match length

**Phase 2: State Splitting for Optimization**
- Identifies states reachable from both the pre-state and other states (indicating potential progress/no-progress ambiguity)  
- Splits such conflicted states into separate progress and no-progress versions to maintain NFA determinism
- Uses the  field to track states in the splitting list
- Copies outgoing arcs to new states and redirects non-pre incoming arcs

This transformation is essential for regex engines that need to find patterns anywhere within input text while maintaining correct backtracking behavior.

## Parameters / Member Variables
- : Pointer to vars structure containing regex compilation context and character map
- : Pointer to the NFA structure to be transformed (must already be optimized)

## Dependencies
- Functions called/Symbols referenced:
  -  - Adds arcs for all characters in colormap
  -  - Creates new NFA arcs
  -  - Creates new NFA states
  -  - Copies outgoing arcs from one state to another
  -  - Copies an arc between specific states
  -  - Deallocates an arc
  -  - Error checking macro
  - Constants: , , , 
  - Structures: , , 
- Called from (representative examples):
  -  (src/backend/regex/regcomp.c:519)
  -  (src/backend/regex/regcomp.c:2379)

## Notes and Other Information
- The NFA must be optimized before calling this function
- Handles both anchored and unanchored patterns appropriately
- The state splitting logic prevents incorrect optimization in cases where states have multiple entry points
- Uses a clever linked list technique with the  field to track states needing splitting
- Maintains MATCHALL semantics but updates maximum match length to infinity for unanchored patterns
- The transformation is essential for implementing proper regex search behavior in text