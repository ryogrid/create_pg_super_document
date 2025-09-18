# onechr

## Location
src/backend/regex/regcomp.c: 1911 - 1936

## Overview
Fills in NFA arcs for a single plain character, handling case-insensitive matching when needed.

## Definition
```c
static void onechr(struct vars *v, chr c, struct state *lp, struct state *rp)
```

## Detailed Description
The `onechr` function creates NFA arcs for a single character, providing an optimized path for the common case of plain character matching. When case-insensitive matching is not enabled (REG_ICASE flag not set), it uses the efficient `subcoloronechr` function to create a single arc. When case-insensitive matching is enabled, it falls back to the general case by calling `allcases` to generate all case variants of the character and then using `subcolorcvec` to create arcs for all variants. This function serves as a performance optimization for the frequent case of matching single characters in regular expressions.

## Parameters / Member Variables
- `v`: Pointer to the vars structure containing compilation flags and NFA state
- `c`: The character to create arcs for
- `lp`: Pointer to the left/start state of the character arc
- `rp`: Pointer to the right/end state of the character arc

## Dependencies
- Functions called/Symbols referenced:
  - REG_ICASE (flag for case-insensitive matching)
  - COLORLESS (color constant)
  - subcoloronechr (creates arc for single character)
  - subcolorcvec (creates arcs for character vector)
  - allcases (generates all case variants of a character)
- Called from:
  - ARCV (main arc processing function)
  - brackpart (for single characters in bracket expressions)

## Notes and Other Information
- Provides performance optimization for the common case of single character matching
- Uses different strategies based on whether case-insensitive matching is enabled
- Fast path uses `subcoloronechr` for case-sensitive matching
- Slower path uses `allcases` + `subcolorcvec` for case-insensitive matching
- Located in src/backend/regex/regcomp.c:1911-1936