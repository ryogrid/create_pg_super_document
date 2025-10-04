# citerdissect

## Location
[src/backend/regex/regexec.c:1117-1320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L1117-L1320)

## Overview
Implements iteration node dissection in regular expression matching by finding valid divisions of the target string into repeated sub-matches and verifying each sub-match recursively.

## Definition

```c
static int						/* regexec return code */
citerdissect(struct vars *v,
			 struct subre *t,
			 chr *begin,		/* beginning of relevant substring */
			 chr *end)			/* end of same */
```
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

## Simplified Source

```c
static int citerdissect(struct vars *v, struct subre *t, chr *begin, chr *end) {
    struct dfa *d;
    chr **endpts;
    chr *limit;
    int min_matches, nverified, k, i, er;
    size_t max_matches;

    // Set up minimum matches (prefer at least 1 even if 0 allowed)
    min_matches = (t->min <= 0) ? 1 : t->min;

    // Calculate workspace size
    max_matches = end - begin;
    if (max_matches > t->max && t->max != DUPINF)
        max_matches = t->max;
    if (max_matches < min_matches)
        max_matches = min_matches;

    // Allocate endpoint tracking array
    endpts = (chr **) MALLOC((max_matches + 1) * sizeof(chr *));
    if (endpts == NULL) return REG_ESPACE;
    endpts[0] = begin;

    d = getsubdfa(v, t->child);
    if (ISERR()) {
        FREE(endpts);
        return v->err;
    }

    // Find valid sub-match divisions and verify them
    nverified = 0;
    k = 1;
    limit = end;

    while (k > 0) {
        // Find endpoint for k'th sub-match
        endpts[k] = longest(v, d, endpts[k - 1], limit, NULL);
        if (endpts[k] == NULL) {
            k--; // Backtrack
            goto backtrack;
        }

        if (nverified >= k) nverified = k - 1;

        if (endpts[k] != end) {
            // Need more iterations if allowed
            if (k >= max_matches ||
                (endpts[k] == endpts[k - 1] &&
                 (k >= min_matches || min_matches - k < end - endpts[k]))) {
                goto backtrack;
            }
            k++;
            limit = end;
            continue;
        }

        // Verify if we have enough matches
        if (k < min_matches) goto backtrack;

        // Verify each sub-match
        for (i = nverified + 1; i <= k; i++) {
            zaptreesubs(v, t->child);
            er = cdissect(v, t->child, endpts[i - 1], endpts[i]);
            if (er == REG_OKAY) {
                nverified = i;
                continue;
            }
            if (er != REG_NOMATCH) {
                FREE(endpts);
                return er;
            }
            break;
        }

        if (i > k) {
            // All verified successfully
            FREE(endpts);
            return REG_OKAY;
        }

        k = i; // Failed at position i

backtrack:
        // Try shorter versions of k'th sub-match
        while (k > 0) {
            chr *prev_end = endpts[k - 1];
            if (endpts[k] > prev_end) {
                limit = endpts[k] - 1;
                if (limit > prev_end ||
                    (k < min_matches && min_matches - k >= end - prev_end))
                    break;
            }
            k--;
        }
    }

    FREE(endpts);

    // Try zero matches if allowed
    if (t->min == 0 && begin == end)
        return REG_OKAY;

    return REG_NOMATCH;
}
```