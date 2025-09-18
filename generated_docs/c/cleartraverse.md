# cleartraverse

## Location
src/backend/regex/regc_nfa.c: 1488 - 1524

## Overview
A recursive cleanup function that traverses an NFA and clears all tmp pointers that were set by other traversal algorithms.

## Definition
```c
static void cleartraverse(struct nfa *nfa,
                          struct state *s)
```

## Detailed Description
This function performs a recursive traversal of the NFA starting from state s, clearing the tmp pointer in each visited state. It serves as a cleanup function for algorithms like dupnfa and removeconstraints that use the tmp pointer for marking visited states or storing temporary data during their operations. The function ensures that the NFA is left in a clean state with no dangling tmp pointers that could interfere with subsequent operations.

## Parameters / Member Variables
- `nfa`: The NFA structure being cleaned up
- `s`: The current state being processed during traversal

## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP
  - NERR
  - REG_ETOOBIG
  - [cleartraverse](cleartraverse.md) (recursive call)
- Called from (representative examples):
  - [dupnfa](../d/dupnfa.md)
  - removeconstraints
  - [cleartraverse](cleartraverse.md) (recursive calls)
  - [cleanup](cleanup.md)

## Notes and Other Information
This is a simple but essential utility function that ensures proper cleanup after complex NFA traversal operations. It includes stack overflow protection like other recursive traversal functions in the regex engine. The function is designed to be safe to call multiple times on the same NFA portion, as it checks if tmp is already NULL before recursing. This cleanup is crucial for maintaining the integrity of the NFA data structure and preventing interference between different algorithmic phases of regex compilation.