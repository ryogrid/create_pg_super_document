# ccondissect

## Location
src/backend/regex/regexec.c: 829 - 909

## Overview
Dissects a match for a concatenation node by finding the optimal split point between left and right subexpressions and recursively matching both parts.

## Definition
static int ccondissect(struct vars *v, struct subre *t, chr *begin, chr *end)

## Detailed Description
The ccondissect function handles the dissection of concatenation nodes in PostgreSQL's regex execution engine. A concatenation node represents a sequence of two subexpressions that must match consecutively (e.g., "ab" where "a" must be followed by "b"). The challenge is finding the correct midpoint where the first subexpression ends and the second begins.

The function operates through an iterative process:
1. It first obtains DFAs (Deterministic Finite Automata) for both the left and right subexpressions
2. Uses the longest() function to find a tentative midpoint where the left subexpression could end
3. Tests if the right subexpression can match from that midpoint to the end
4. If both subexpressions match successfully, it calls cdissect() recursively on both parts
5. If matching fails, it backtracks by finding a shorter match for the left part and tries again
6. The process continues until either a successful match is found or all possibilities are exhausted

The function includes proper cleanup of match data using zaptreesubs() when backtracking occurs, ensuring that failed partial matches don't interfere with subsequent attempts.

## Parameters / Member Variables
- v: Pointer to a vars structure containing regex execution state and match results
- t: Pointer to a subre structure representing the concatenation node
- begin: Pointer to the beginning character of the substring to match
- end: Pointer to the end character (exclusive) of the substring to match

## Dependencies
- Functions called/Symbols referenced:
  - struct vars, struct subre, struct dfa, chr (core regex data structures)
  - [cnfa](cnfa.md) (compiled NFA structure)
  - getsubdfa (function to obtain DFA for subexpression)
  - [longest](../l/longest.md) (function to find longest match)
  - [cdissect](cdissect.md) (recursive dissection function)
  - [zaptreesubs](../z/zaptreesubs.md) (function to clear match data)
  - NOERR, MDEBUG, LOFF (utility/debugging macros)
  - REG_OKAY, REG_NOMATCH, REG_ASSERT (return codes)
  - SHORTER (flag constant)
- Called from (representative examples):
  - LOFF (macro/function at src/backend/regex/regexec.c:150)
  - [cdissect](cdissect.md) (function at src/backend/regex/regexec.c:787)

## Notes and Other Information
- This is a static function, only accessible within regexec.c
- Specifically handles concatenation nodes (op == ".")
- Assumes left subexpression does not have the SHORTER flag set (forward matching)
- Uses an iterative backtracking approach to find the correct split point
- Includes extensive debug logging to trace the matching process
- The function ensures proper cleanup of partial matches during backtracking
- Critical for handling complex regex patterns with multiple consecutive elements
- The algorithm is designed to be greedy initially, then backtrack as needed
- Part of PostgreSQL's sophisticated regex matching engine that handles both simple and complex patterns