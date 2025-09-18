# cbracket

## Location
[src/backend/regex/regcomp.c:1729-1762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L1729-L1762)

## Overview
Handles complemented bracket expressions in regular expression compilation by delegating to bracket() and then complementing the result.

## Definition
```c
static void cbracket(struct vars *v, struct state *lp, struct state *rp)
```

## Detailed Description
The `cbracket` function processes complemented bracket expressions (like `[^abc]` or `[^a-z]`) in regular expressions. It uses a clever approach: instead of directly building the complement, it calls the `bracket` function with dummy temporary states to build the positive bracket expression, then uses `colorcomplement` to create the complement. This approach is simpler than the alternative of starting with a rainbow and deleting arcs. In NLSTOP mode, it ensures newlines are excluded from the result set. The function handles cleanup of temporary states after processing.

## Parameters / Member Variables
- `v`: Pointer to the vars structure containing compilation state and NFA information
- `lp`: Pointer to the left/start state of the complemented bracket expression
- `rp`: Pointer to the right/end state of the complemented bracket expression

## Dependencies
- Functions called/Symbols referenced:
  - [newstate](../n/newstate.md) (creates new NFA states)
  - NOERR (error handling macro)
  - [bracket](../b/bracket.md) (processes the positive bracket expression)
  - REG_NLSTOP (flag for newline stop mode)
  - [newarc](../n/newarc.md) (creates new NFA arc)
  - PLAIN (arc type constant)
  - [colorcomplement](colorcomplement.md) (complements the color set)
  - [dropstate](../d/dropstate.md) (removes state from NFA)
  - [freestate](../f/freestate.md) (deallocates state memory)
- Called from:
  - ARCV (main arc processing function)

## Notes and Other Information
- Uses temporary states to delegate positive bracket processing to the `bracket` function
- Implements complementation through `colorcomplement` rather than direct construction
- Handles NLSTOP mode by explicitly excluding newlines from the complement
- The result cannot be a rainbow since empty brackets are not allowed
- Performs proper cleanup of temporary states to avoid memory leaks
- Located in src/backend/regex/regcomp.c:1729-1762