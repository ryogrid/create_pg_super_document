# DeadLockCheckRecurse

## Location
src/backend/storage/lmgr/deadlock.c: 309 - 374

## Overview
Recursively searches for valid lock queue orderings to resolve deadlocks by testing different constraint combinations.

## Definition
static bool DeadLockCheckRecurse(PGPROC *proc)

## Detailed Description
DeadLockCheckRecurse implements the core recursive algorithm for deadlock resolution. It explores different possible constraint combinations to find a configuration that breaks deadlock cycles while maintaining lock safety properties. The function works by:

1. Testing the current configuration using TestConfiguration to identify constraint edges
2. If no edges exist, a valid (deadlock-free) configuration is found
3. If edges exist, recursively try each edge as an additional constraint
4. The recursion explores all possible combinations until a solution is found or all possibilities are exhausted

The function maintains constraint lists and manages memory efficiently by optionally saving edge lists or regenerating them on-the-fly depending on available space. The recursion depth is limited by maxCurConstraints to prevent stack overflow.

## Parameters / Member Variables
- : Pointer to the PGPROC structure representing the process being checked for deadlocks

## Dependencies
- Functions called/Symbols referenced:
  - TestConfiguration
  - DeadLockCheckRecurse (recursive call)
  - PGPROC (struct type)
  - curConstraints (global array)
  - possibleConstraints (global array)
  - nCurConstraints (global variable)
  - nPossibleConstraints (global variable)
  - maxCurConstraints (global variable)
- Called from (representative examples):
  - DeadLockCheck
  - DeadLockCheckRecurse (recursive)

## Notes and Other Information
- Returns true if no solution exists (hard deadlock), false if a deadlock-free state is attainable
- Implements a backtracking algorithm that tries different constraint combinations
- The recursion depth is bounded by maxCurConstraints to prevent stack overflow
- Memory management optimizes by saving edge lists when space allows, or regenerating them when needed
- The function is static (internal to deadlock.c) and part of the deadlock resolution algorithm
- Each recursive level adds one constraint and explores all possibilities at that level before backtracking
- The algorithm ensures that all possible solutions are explored systematically