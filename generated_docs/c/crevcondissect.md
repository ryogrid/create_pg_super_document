# crevcondissect

## Location
[src/backend/regex/regexec.c:910-993](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L910-L993)

## Overview
Implements the dissection algorithm for concatenation nodes in regular expression matching using a shortest-first strategy for the left operand.

## Definition

```c
static int						/* regexec return code */
crevcondissect(struct vars *v,
			   struct subre *t,
			   chr *begin,		/* beginning of relevant substring */
			   chr *end)		/* end of same */
```
## Detailed Description
The  function is a specialized dissection function for concatenation operators ('.') in regular expression parsing. It employs a shortest-first approach where the left child of the concatenation is matched with the shortest possible substring first, then the right child is tested with the remaining portion. This is the reverse strategy compared to , which uses a longest-first approach for the left operand.

The function works by iteratively finding tentative midpoints using the  function for the left operand, then verifying if the right operand can match from that midpoint to the end. If both sides match successfully, the function returns success. If not, it advances to the next possible shortest match for the left side and repeats the process.

## Parameters
- : Pointer to the vars struct containing regex execution context and state information
- : Pointer to the subre (subexpression) struct representing the concatenation node being processed
- : Pointer to the beginning character of the substring to match
- : Pointer to the end character of the substring to match

## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves the DFA for subexpressions
  - : Finds the shortest possible match for a DFA
  - : Finds the longest possible match for a DFA
  - : Recursively dissects child subexpressions
  - : Resets subexpression matches
  - : Macro for error checking
  - : Macro for debug output
  - : Macro for converting pointers to offsets
- Called from:
  - : Main concatenation dissection dispatcher function

## Notes and Other Information
- This function is specifically designed for concatenation nodes where the left operand has the SHORTER flag set
- The algorithm ensures all possible combinations are tried by advancing the midpoint when a match fails
- Error handling includes checking for REG_NOMATCH, REG_OKAY, and other regex execution return codes
- The function includes extensive debug output to trace the matching process
- Performance optimization through early termination when no valid midpoint can be found

## Simplified Source

```c
static int crevcondissect(struct vars *v, struct subre *t, chr *begin, chr *end) {
    struct subre *left = t->child;
    struct subre *right = left->sibling;
    struct dfa *d, *d2;
    chr *mid;
    int er;

    // Get DFAs for left and right subexpressions
    d = getsubdfa(v, left);
    d2 = getsubdfa(v, right);

    // Find initial midpoint using shortest match for left side
    mid = shortest(v, d, begin, begin, end, NULL, NULL);
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

        // Find longer match for left side (shortest-first strategy)
        if (mid == end)
            return REG_NOMATCH;  // No more possibilities

        mid = shortest(v, d, begin, mid + 1, end, NULL, NULL);
        if (mid == NULL)
            return REG_NOMATCH;
    }
}
```