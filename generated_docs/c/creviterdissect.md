# creviterdissect

## Location
[src/backend/regex/regexec.c:1321-1514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L1321-L1514)

## Overview
Implements iteration node dissection in regular expression matching using a shortest-first strategy for finding sub-match divisions, designed for child patterns with the SHORTER flag.

## Definition


## Detailed Description
The  function is the counterpart to , designed specifically for iteration nodes where the child pattern has the SHORTER flag set. It implements a shortest-first strategy for dividing the target string into repeated sub-matches.

Key differences from :
1. **Early zero-match handling**: If zero matches are allowed and the target string is empty, it immediately returns success
2. **Shortest-first approach**: Uses the  function instead of  to find minimal sub-matches first
3. **Different backtracking strategy**: When backtracking, it tries to lengthen previous matches rather than shorten them
4. **Zero-length match prevention**: More aggressive in avoiding zero-length matches unless necessary for meeting minimum requirements

The algorithm follows the same two-phase approach as  but with reversed preferences, making it suitable for non-greedy quantifiers and patterns that should prefer shorter matches.

## Parameters
- : Pointer to the vars struct containing regex execution context and memory management functions
- : Pointer to the subre (subexpression) struct representing the iteration node with min/max bounds
- : Pointer to the beginning character of the substring to match against the iteration
- : Pointer to the end character of the substring to match

## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves the DFA representation for the child subexpression
  - : Finds the shortest possible match for a DFA within specified bounds
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
- Specifically designed for iteration nodes where the child has the SHORTER flag set
- Implements shortest-first matching strategy, opposite to 's longest-first approach
- Handles zero matches more proactively when the target string is empty
- Uses different backtracking logic that tries to extend rather than contract previous matches
- Includes sophisticated logic to avoid unnecessary zero-length matches
- The function ensures that when multiple valid divisions exist, the one with shortest individual sub-matches is preferred
- Critical for implementing non-greedy quantifiers like '*?', '+?', and '{m,n}?' in regular expressions