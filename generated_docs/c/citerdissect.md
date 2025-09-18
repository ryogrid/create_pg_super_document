# citerdissect

## Location
src/backend/regex/regexec.c: 1117 - 1320

## Overview
Implements iteration node dissection in regular expression matching by finding valid divisions of the target string into repeated sub-matches and verifying each sub-match recursively.

## Definition


## Detailed Description
The  function handles iteration nodes ('*', '+', '?', '{m,n}') in regular expression matching. It implements a sophisticated two-phase algorithm:

**Phase 1: Finding candidate sub-match boundaries**
- Uses the child node's DFA to identify potential endpoints for each repetition
- Employs a backtracking strategy to find valid divisions of the target string
- Handles constraints on minimum and maximum repetition counts
- Optimizes by preferring non-zero-length matches when possible

**Phase 2: Verification**
- Recursively calls  to verify that each sub-match actually matches the child pattern
- Uses incremental verification to avoid re-checking previously verified sub-matches
- Implements sophisticated backtracking when verification fails

The function includes special handling for zero-length matches and zero repetitions, with preference for non-empty matches when possible to ensure capturing groups are properly set.

## Parameters
- : Pointer to the vars struct containing regex execution context and memory management functions
- : Pointer to the subre (subexpression) struct representing the iteration node with min/max bounds
- : Pointer to the beginning character of the substring to match against the iteration
- : Pointer to the end character of the substring to match

## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves the DFA representation for the child subexpression
  - : Finds the longest possible match for a DFA from a starting position
  - : Recursively dissects child subexpressions for verification
  - : Resets subexpression matches between attempts
  - /: Memory allocation and deallocation
  - : Macro for error checking
  - : Macro for debug output
  - : Macro for converting pointers to offsets
  - : Constant representing infinite repetitions
  - //: Return codes
- Called from:
  - : Main dissection dispatcher function

## Notes and Other Information
- The function expects iteration nodes to be identified by op == '*'
- Child nodes must have valid CNFAs and cannot have the SHORTER flag set
- Implements memory management for tracking endpoint arrays with proper cleanup on errors
- Uses sophisticated backtracking with state preservation (nverified counter)
- Handles edge cases including zero-length strings, zero repetitions, and minimum repetition requirements
- Prefers to match at least once even when zero matches are allowed, to ensure capturing groups are set
- The algorithm is designed to handle complex nested patterns with multiple possible valid divisions