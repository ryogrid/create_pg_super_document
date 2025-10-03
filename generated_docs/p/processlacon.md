# processlacon

## Location
[src/backend/regex/regcomp.c:2029-2094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L2029-L2094)

## Overview
Generates the NFA representation of a LACON (lookaround constraint) by optimizing simple cases or creating general LACON arcs for complex lookahead and lookbehind assertions.

## Definition

```c
static void
processlacon(struct vars *v,
			 struct state *begin,	/* start of parsed LACON sub-re */
			 struct state *end, /* end of parsed LACON sub-re */
			 int latype,
			 struct state *lp,	/* left state to hang it on */
			 struct state *rp)	/* right state to hang it on */
```
## Detailed Description
The processlacon function handles the conversion of parsed lookaround assertions (lookahead and lookbehind) into their corresponding NFA representation. It implements significant optimizations for simple cases where the lookaround consists of just a single color or character set, converting them into more efficient arc types rather than full LACON constructs.

The function handles four types of lookaround assertions:
- Positive lookahead (LATYPE_AHEAD_POS): (?=...)
- Negative lookahead (LATYPE_AHEAD_NEG): (?!...)  
- Positive lookbehind (LATYPE_BEHIND_POS): (?<=...)
- Negative lookbehind (LATYPE_BEHIND_NEG): (?<!...)

For simple cases (single color transitions), it creates optimized arcs:
- Positive assertions become AHEAD/BEHIND arcs with the color set
- Negative assertions become complement arcs plus boundary markers (^ or $)

For complex cases that cannot be optimized, it falls back to creating a general LACON arc that references a subre (sub-regular expression).

## Parameters / Member Variables
- `*v`: Pointer to vars structure containing regex compilation state
- `*begin`: Pointer to the start state of the parsed LACON sub-regular expression
- `*end`: Pointer to the end state of the parsed LACON sub-regular expression
- `latype`: Integer indicating the type of lookaround (LATYPE_AHEAD_POS, LATYPE_AHEAD_NEG, LATYPE_BEHIND_POS, LATYPE_BEHIND_NEG)
- `*lp`: Pointer to the left state where the LACON should be attached
- `*rp`: Pointer to the right state where the LACON should be attached
## Dependencies
- Functions called/Symbols referenced:
  - [single_color_transition](../s/single_color_transition.md) (checks if RE is a simple color transition)
  - [cloneouts](../c/cloneouts.md) (copies outgoing arcs with specified type)
  - [colorcomplement](../c/colorcomplement.md) (creates complement of color set)
  - [newarc](../n/newarc.md) (creates new NFA arc)
  - [newlacon](../n/newlacon.md) (creates new LACON subre for general case)
- Data structures used:
  - [state](../s/state.md) (NFA state structure)
  - [subre](../s/subre.md) (sub-regular expression structure)
- Constants used:
  - LATYPE_AHEAD_POS, LATYPE_AHEAD_NEG, LATYPE_BEHIND_POS, LATYPE_BEHIND_NEG (lookaround types)
  - AHEAD, BEHIND, LACON (arc types)
  - NOTREACHED (assertion macro)
- Called from:
  - ARCV macro in regcomp.c:953

## Notes and Other Information
- Implements important optimizations that convert simple lookaround patterns into efficient arc types rather than full LACON processing
- Negative lookaround assertions include boundary markers (^ for lookbehind, $ for lookahead) to handle edge cases
- The optimization detection relies on single_color_transition to identify simple patterns
- Falls back to general LACON processing for complex lookaround expressions that cannot be optimized
- LACON arcs reference numbered subreges for complex lookaround evaluation during matching