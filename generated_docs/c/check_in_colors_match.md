# check_in_colors_match

## Location
[src/backend/regex/regc_nfa.c:3469-3513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L3469-L3513)

## Overview
A static helper function that compares whether two colors leading into a given state originate from equivalent sets of source states in a regular expression NFA.

## Definition

```c
static bool
check_in_colors_match(struct state *s, color co1, color co2)
```
## Detailed Description
This function is a subroutine for  that determines if two colors (co1 and co2) leading into a given state come from equivalent sets of source states. It performs the inverse operation of , examining incoming arcs instead of outgoing arcs.

The algorithm works in three passes through the incoming arcs:
1. First pass: Mark all states that can reach  via arcs of color  using the  field
2. Second pass: For arcs of color , unmark matching states or flag as unmatched if the source wasn't marked
3. Third pass: Check for any remaining marked states from  arcs, indicating unmatched  arcs

Like its counterpart function, it assumes the NFA contains no duplicate arcs and that all arcs are PLAIN type (as verified by the caller ).

## Parameters / Member Variables
- `*s`: The destination state to which incoming arcs are compared
- `co1`: The first color to compare
- `co2`: The second color to compare
## Dependencies
- Functions called/Symbols referenced:
  -  (typedef)
  -  (struct)
  -  (struct)
- Called from (representative examples):
  -  (at src/backend/regex/regc_nfa.c:3163, 3164)

## Notes and Other Information
- This function is part of PostgreSQL's regular expression engine optimization
- Uses identical algorithm to  but examines incoming arcs () instead of outgoing arcs ()
- Uses temporary marking via the  field in state structures to achieve linear time complexity
- Critical for determining when colors can be merged during NFA optimization from the reverse direction
- Assumes no duplicate arcs exist in the NFA structure
- Always resets the  fields to NULL before returning to maintain clean state