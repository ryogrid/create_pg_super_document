# single_color_transition

## Location
[src/backend/regex/regc_nfa.c:1525-1554](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L1525-L1554)

## Overview
Determines whether traversing from one state to another requires exactly one PLAIN arc transition, returning the state containing the relevant outbound arcs if true.

## Definition
```c
static struct state *single_color_transition(struct state *s1, struct state *s2)
```

## Detailed Description
This function analyzes whether getting from state s1 to state s2 crosses exactly one PLAIN arc (possibly matching any of a set of colors). It handles EMPTY arcs by skipping over leading EMPTY arcs from s1 and trailing EMPTY arcs to s2, since these are optimization artifacts that should be ignored. The function is designed to work before NFA optimization when EMPTY arcs may still be present. It returns the state whose outarc list contains only PLAIN arcs of the required color(s), or NULL if the transition doesn't meet the criteria.

## Parameters / Member Variables
- `s1`: The source state for the transition
- `s2`: The destination state for the transition

## Dependencies
- Functions called/Symbols referenced:
  - EMPTY (arc type constant)
  - PLAIN (arc type constant)
- Called from (representative examples):
  - processlacon

## Notes and Other Information
The function is specifically designed to handle bracket constructs like [abc] which might yield either one or several parallel PLAIN arcs depending on earlier atoms in the expression. This ensures that implementation details don't create user-visible performance differences. The function performs several validations: it skips over EMPTY arcs, rejects single-state loops, ensures s1 has outbound arcs, and verifies that all outbound arcs from s1 are PLAIN arcs leading to s2. This is used in the context of regex optimization to identify simple color transitions that can be optimized.