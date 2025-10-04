# ccondissect

## Location
[src/backend/regex/regexec.c:829-909](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L829-L909)

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
  - [getsubdfa](../g/getsubdfa.md) (function to obtain DFA for subexpression)
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

## Simplified Source

```c
static int ccondissect(struct vars *v, struct subre *t, chr *begin, chr *end) {
    struct subre *left = t->child;
    struct subre *right = left->sibling;
    struct dfa *d, *d2;
    chr *mid;
    int er;

    // Get DFAs for left and right subexpressions
    d = getsubdfa(v, left);
    d2 = getsubdfa(v, right);

    // Find initial midpoint using longest match for left side
    mid = longest(v, d, begin, end, NULL);
    if (mid == NULL)
        return REG_NOMATCH;

    // Try different midpoints until we find one that works
    for (;;) {
        // Test if right side can match from midpoint to end
        if (longest(v, d2, mid, end, NULL) == end) {
            // Try to dissect both left and right parts
            er = cdissect(v, left, begin, mid);
            if (er == REG_OKAY) {
                er = cdissect(v, right, mid, end);
                if (er == REG_OKAY)
                    return REG_OKAY;  // Success!

                // Reset left's matches on failure
                zaptreesubs(v, left);
            }
            if (er != REG_NOMATCH)
                return er;
        }

        // Find shorter match for left side
        if (mid == begin)
            return REG_NOMATCH;  // No more possibilities

        mid = longest(v, d, begin, mid - 1, NULL);
        if (mid == NULL)
            return REG_NOMATCH;
    }
}
```