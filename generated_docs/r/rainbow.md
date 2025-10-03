# rainbow

## Location
[src/backend/regex/regc_color.c:1031-1063](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L1031-L1063)

## Overview
The  function creates arcs for all full colors (except optionally one) between specified states, using either a special RAINBOW arc or individual color arcs for optimization.

## Definition

```c
static void
rainbow(struct nfa *nfa,
		struct colormap *cm,
		int type,
		color but,				/* COLORLESS if no exceptions */
		struct state *from,
		struct state *to)
```
## Detailed Description
This function implements an important optimization in regular expression compilation by creating arcs that match "all colors except one" (or truly all colors). When no exception color is specified (but == COLORLESS), it creates a single special RAINBOW arc, which is much more efficient than creating individual arcs for each color.

When an exception color is specified, the function falls back to the "hard way" of creating individual arcs for each valid color, excluding subcolors, pseudocolors, and the specified exception color. This functionality is essential for implementing constructs like negated character classes (e.g., [^a] which matches everything except 'a').

The RAINBOW optimization significantly reduces the number of arcs in the NFA, which improves both compilation time and runtime matching performance.

## Parameters / Member Variables
- `*nfa`: Pointer to the NFA structure where arcs will be created
- `*cm`: Pointer to the colormap structure containing color information
- `type`: The type of arc to create (e.g., PLAIN)
- `but`: Exception color to exclude, or COLORLESS if no exceptions
- `*from`: Source state for the arcs
- `*to`: Destination state for the arcs
## Dependencies
- Functions called/Symbols referenced:
  - : Creates new arcs in the NFA
  - : Macro to get the end of color descriptor array
  - : Constant indicating no color specified
  - : Special color constant for all-colors arc
  - : Error checking macro
  - : Macro to check if a color is unused
  - : Flag indicating pseudocolors
- Called from (representative examples):
  - : During NFA initialization
  - : When creating search patterns
  - : Arc creation functions

## Notes and Other Information
- This is a static helper function used internally within the regex color processing module
- Key optimization: uses single RAINBOW arc when no exceptions are needed
- Skips subcolors and pseudocolors when creating individual arcs
- Critical for implementing negated character classes efficiently
- Part of the arc creation optimization system that reduces NFA complexity
- The RAINBOW arc type is a special optimization that represents "match any color"