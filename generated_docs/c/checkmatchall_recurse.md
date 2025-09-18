# checkmatchall_recurse

## Location
[src/backend/regex/regc_nfa.c:3277-3414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L3277-L3414)

## Overview
The checkmatchall_recurse function is a recursive helper function for checkmatchall that performs depth-first traversal of an NFA to compute all possible path lengths from each state to the post state.

## Definition


## Detailed Description
This function performs the core recursive analysis for matchall detection by exploring all possible RAINBOW (any-character) paths from a given state to the post state. It builds a comprehensive map of possible path lengths, handling various edge cases including loops and infinite path lengths.

The function maintains a per-state array (haspath) that tracks all possible path lengths from the current state to the end. The array has DUPINF+2 elements: indices 0 to DUPINF represent specific path lengths, and index DUPINF+1 represents "infinite" length (all lengths >= DUPINF+1 are possible).

Key features include:
- Stack overflow protection to prevent deep recursion crashes
- Cycle detection to handle self-loops and multi-state loops differently  
- Memoization through the haspaths array to avoid redundant computation
- Special handling of length-1 loops that make all longer paths possible
- Rejection of multi-state loops which complicate length analysis

The function marks states as "busy" during traversal to detect cycles and prevent infinite recursion.

## Parameters / Member Variables
- : Pointer to the NFA structure being analyzed
- : The current state to analyze path lengths from
- : Array of per-state path length arrays for memoization

## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP (stack overflow protection macro)
  - INTERRUPT (cancellation check macro)
  - MALLOC (memory allocation)
  - DUPINF (maximum finite duplication count)
  - RAINBOW (color constant for any-character arcs)
  - [checkmatchall_recurse](checkmatchall_recurse.md) (recursive self-call)
- Called from (representative examples):
  - [checkmatchall](checkmatchall.md) (src/backend/regex/regc_nfa.c:3185)
  - [checkmatchall_recurse](checkmatchall_recurse.md) (self-recursive call at 3339)
  - REPLACEARC macro (src/backend/regex/regcomp.c:226)

## Notes and Other Information
- This is a static function, only accessible within the regc_nfa.c file
- Returns true on successful analysis, false if the NFA cannot be represented as a matchall pattern
- Uses sophisticated cycle detection: single-state loops are handled gracefully, multi-state loops cause failure
- The function implements memoization to prevent exponential time complexity
- Critical for enabling regex engine optimizations when patterns only test string length
- Handles the complex case where infinite path lengths must be represented within finite data structures
- Memory allocated for haspath arrays is managed by the calling checkmatchall function