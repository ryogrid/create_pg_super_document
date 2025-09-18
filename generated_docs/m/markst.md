# markst

## Location
src/backend/regex/regcomp.c: 2297 - 2311

## Overview
A recursive tree traversal function that marks all reachable subRE nodes as INUSE, preparing them for the cleanup phase in regex compilation.

## Definition
```c
static void markst(struct subre *t)
```

## Detailed Description
This function plays a critical role in PostgreSQL's regex compilation memory management strategy. It recursively traverses the subRE tree starting from the root and marks all reachable nodes with the INUSE flag. This marking phase is essential for the subsequent cleanup process that will determine which subRE nodes should be preserved and which can be discarded.

The function operates as part of a two-phase cleanup process (markst followed by cleanst) that transitions the regex compiler from its initial parsing state to a final optimized state. During parsing, all subREs are maintained in both treechain and treefree lists, but after this marking phase, only the tree structure itself is used for navigation. This transition changes how memory management works throughout the rest of the compilation process.

## Parameters / Member Variables
- `t`: Pointer to the subRE node to mark (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [markst](markst.md): Recursive self-call to process child nodes
  - `INUSE`: Flag indicating the node is actively used
  - `subre`: Sub-regular expression structure type
- Called from (representative examples):
  - `CNOERR`: Main compilation flow
  - [markst](markst.md): Recursive self-calls for tree traversal

## Notes and Other Information
- Must be called before cleanst() as part of the two-phase cleanup process
- Critical for proper memory management during regex compilation state transition
- The function assumes input is never NULL and will assert if it is
- Part of a sophisticated memory management strategy that prevents dangling pointers
- Changes the behavior of freesubre() for all subsequent operations
- Error handling must be carefully managed during the markst/cleanst transition period
- Used exclusively during regex compilation, not during execution