# creviterdissect

## Location
[src/backend/regex/regexec.c:1321-1514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L1321-L1514)

## Overview
Implements iteration node dissection in regular expression matching using a shortest-first strategy for finding sub-match divisions, designed for child patterns with the SHORTER flag.

## Definition

```c
static int						/* regexec return code */
creviterdissect(struct vars *v,
				struct subre *t,
				chr *begin,		/* beginning of relevant substring */
				chr *end)		/* end of same */
```
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

## Simplified Source

```c
static int creviterdissect(struct vars *v, struct subre *t, chr *begin, chr *end) {
    struct dfa *d;
    chr **endpts;
    chr *limit;
    int min_matches, nverified, k, i, er;
    size_t max_matches;

    // Handle zero matches early if allowed and string is empty
    min_matches = t->min;
    if (min_matches <= 0) {
        if (begin == end)
            return REG_OKAY;
        min_matches = 1;
    }

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

    // Find valid sub-match divisions using shortest-first strategy
    nverified = 0;
    k = 1;
    limit = begin;

    while (k > 0) {
        // Avoid zero-length matches unless necessary
        if (limit == endpts[k - 1] && limit != end &&
            (k >= min_matches || min_matches - k < end - limit))
            limit++;

        // Force last sub-match to reach the end
        if (k >= max_matches)
            limit = end;

        // Find shortest endpoint for k'th sub-match
        endpts[k] = shortest(v, d, endpts[k - 1], limit, end, NULL, NULL);
        if (endpts[k] == NULL) {
            k--; // Backtrack
            goto backtrack;
        }

        if (nverified >= k) nverified = k - 1;

        if (endpts[k] != end) {
            // Need more iterations if allowed
            if (k >= max_matches) {
                k--;
                goto backtrack;
            }
            k++;
            limit = endpts[k - 1];
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
        // Try longer versions of k'th sub-match
        while (k > 0) {
            if (endpts[k] < end) {
                limit = endpts[k] + 1;
                break;
            }
            k--;
        }
    }

    FREE(endpts);
    return REG_NOMATCH;
}
```