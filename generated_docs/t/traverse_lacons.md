# traverse_lacons

## Location
[src/backend/regex/regexport.c:93-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexport.c#L93-L133)

## Overview
A recursive function that traverses LACON (Look Ahead Constraints) arcs in an NFA to count and collect reachable regular arcs, masking LACON complexity from external callers.

## Definition
```c
static void traverse_lacons(struct cnfa *cnfa, int st, int *arcs_count, regex_arc_t *arcs, int arcs_len)
```

## Detailed Description
This internal function recursively traverses LACON arcs in a compiled NFA to find all reachable regular (non-LACON) arcs from a given state. LACON arcs represent look-ahead constraints that don't consume characters, and this function treats them as automatically satisfied. The function serves as a helper for the exported functions `pg_reg_getnumoutarcs()` and `pg_reg_getoutarcs()`, allowing them to present a simplified view of the NFA where LACON arcs are transparent to the caller.

When the function encounters an ordinary arc (color < ncolors), it increments the count and optionally stores the arc information. When it encounters a LACON arc, it recursively follows it to find reachable ordinary arcs. The function includes stack depth checking to prevent overflow from potential LACON loops.

## Parameters / Member Variables
- `cnfa`: Pointer to the compiled NFA structure containing state and arc information
- `st`: The current state identifier from which to traverse outgoing arcs
- `arcs_count`: Pointer to counter that tracks the total number of reachable regular arcs found
- `arcs`: Array to store arc information (may be NULL for counting-only operations)
- `arcs_len`: Maximum number of arcs that can be stored in the arcs array (may be 0)

## Dependencies
- Functions called/Symbols referenced:
  - `[check_stack_depth](../c/check_stack_depth.md)` (stack overflow protection)
  - [cnfa](../c/cnfa.md) (compiled NFA structure)
  - `[carc](../c/carc.md)` (compiled arc structure)
  - `COLORLESS` (sentinel value for arc termination)
  - [regex_arc_t](../r/regex_arc_t.md) (output arc structure)
  - [traverse_lacons](traverse_lacons.md) (recursive self-call)
- Called from (representative examples):
  - [pg_reg_getnumoutarcs](../p/pg_reg_getnumoutarcs.md) (to count outgoing arcs)
  - [pg_reg_getoutarcs](../p/pg_reg_getoutarcs.md) (to retrieve arc information)

## Notes and Other Information
- This is a static (internal) function not exposed to external callers
- The function is recursive and includes stack depth checking to prevent overflow
- LACON arcs never lead directly to the final state, which is enforced by an assertion
- The design handles the impedance mismatch between the internal NFA representation (which includes LACON arcs) and the external API (which presents only regular arcs)
- The function efficiently combines counting and emission phases - arcs are stored only if there's space in the output array
- The recursive traversal assumes LACON constraints are satisfied, simplifying the external API

## Simplified Source

```c
static void traverse_lacons(struct cnfa *cnfa, int st,
                           int *arcs_count,
                           regex_arc_t *arcs, int arcs_len) {
    struct carc *ca;

    // Prevent stack overflow from potential LACON loops
    check_stack_depth();

    // Examine each arc from current state
    for (ca = cnfa->states[st]; ca->co != COLORLESS; ca++) {
        if (ca->co < cnfa->ncolors) {
            // Regular arc - count and possibly store it
            int ndx = (*arcs_count)++;

            if (ndx < arcs_len) {
                arcs[ndx].co = ca->co;
                arcs[ndx].to = ca->to;
            }
        } else {
            // LACON arc - recursively follow it
            // Assert it doesn't lead to final state
            Assert(ca->to != cnfa->post);
            traverse_lacons(cnfa, ca->to, arcs_count, arcs, arcs_len);
        }
    }
}
```