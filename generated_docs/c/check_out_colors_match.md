# check_out_colors_match

## Location
[src/backend/regex/regc_nfa.c:3415-3468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L3415-L3468)

## Overview
A static helper function that compares whether two colors from a given state lead to equivalent sets of destination states in a regular expression NFA (Non-deterministic Finite Automaton).

## Definition

```c
static bool
check_out_colors_match(struct state *s, color co1, color co2)
```
## Detailed Description
This function is a subroutine for  that determines if two colors (co1 and co2) from a given state produce equivalent transition patterns. It performs a linear-time comparison by examining the outgoing arcs from state  and checking whether arcs of color  reach the same set of destination states as arcs of color .

The algorithm works in three passes through the outgoing arcs:
1. First pass: Mark all states reachable via arcs of color  using the  field
2. Second pass: For arcs of color , unmark matching states or flag as unmatched if the destination wasn't marked
3. Third pass: Check for any remaining marked states from  arcs, indicating unmatched  arcs

The function assumes the NFA contains no duplicate arcs and that all arcs are PLAIN type (as verified by the caller ).

## Parameters / Member Variables
- : The source state from which to compare outgoing arcs
- : The first color to compare
- : The second color to compare

## Dependencies
- Functions called/Symbols referenced:
  -  (typedef)
  -  (struct)
  -  (struct)
- Called from (representative examples):
  -  (at src/backend/regex/regc_nfa.c:3161, 3162)

## Notes and Other Information
- This function is part of PostgreSQL's regular expression engine optimization
- Uses temporary marking via the  field in state structures to achieve linear time complexity
- Critical for determining when colors can be merged during NFA optimization
- Assumes no duplicate arcs exist in the NFA structure
- Always resets the  fields to NULL before returning to maintain clean state