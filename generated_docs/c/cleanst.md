# cleanst

## Location
[src/backend/regex/regcomp.c:2312-2330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L2312-L2330)

## Overview
The cleanup function that frees all subRE nodes not marked as INUSE and finalizes the regex compilation memory management transition.

## Definition
```c
static void cleanst(struct vars *v)
```

## Detailed Description
This function completes the two-phase cleanup process in PostgreSQL's regex compilation, working in conjunction with markst() to finalize memory management. It traverses the treechain list and frees all subRE nodes that were not marked with the INUSE flag during the marking phase. After cleanup, it clears both the treechain and treefree pointers, officially transitioning the regex compiler from its parsing state to its final optimized state.

This transition is crucial because it changes how subsequent memory operations work throughout the regex engine. After cleanst() completes, all subRE navigation must be done through the tree structure itself, and any future subRE deallocations will use direct FREE() calls rather than the reuse pool mechanism used during parsing.

## Parameters / Member Variables
- `v`: Pointer to the vars structure containing the treechain and treefree lists to be cleaned

## Dependencies
- Functions called/Symbols referenced:
  - `FREE`: Memory deallocation macro for unused nodes
  - `INUSE`: Flag indicating nodes to preserve
  - `[subre](../s/subre.md)`: Sub-regular expression structure type
- Called from (representative examples):
  - `CNOERR`: Main compilation flow after markst()
  - [freev](../f/freev.md): Variable cleanup during error handling

## Notes and Other Information
- Must be called after markst() as the second phase of the cleanup process
- Finalizes the regex compilation memory management state transition
- Clears both treechain and treefree lists to prevent future access
- Changes the behavior of all subsequent freesubre() calls
- Critical for preventing memory leaks of unreachable subRE nodes
- Part of PostgreSQL's sophisticated regex compilation memory management strategy
- After this function, tree navigation relies solely on parent-child-sibling relationships