# spgdoinsert

## Location
src/backend/access/spgist/spgdoinsert.c: 1914 - 2357

## Overview
Primary insertion function for SP-GiST that coordinates the complete process of inserting a tuple into the index, handling tree navigation, space management, and various insertion scenarios.

## Definition


## Detailed Description
The  function orchestrates the entire SP-GiST tuple insertion process. It begins by preparing the leaf tuple data, optionally applying compression, and validating size constraints. The function then navigates the tree starting from the appropriate root (null or regular), following opclass-defined choose function guidance. At each inner node, it may perform match (descend), addNode (expand inner tuple), or splitTuple (restructure inner tuple) operations. When reaching a leaf page, it either directly inserts the tuple, moves the entire leaf chain to a new page, or performs a picksplit operation to redistribute tuples across multiple pages.

## Parameters / Member Variables
- : The SP-GiST index relation to insert into
- : SP-GiST state containing opclass information and configuration
- : Item pointer to the heap tuple being indexed
- : Array of column values for the index tuple
- : Array of null flags corresponding to datums

## Dependencies
- Functions called/Symbols referenced:
  - [SpGistGetLeafTupleSize](../S/SpGistGetLeafTupleSize.md)
  - [spgFormLeafTuple](spgFormLeafTuple.md)
  - [addLeafTuple](../a/addLeafTuple.md)
  - [checkSplitConditions](../c/checkSplitConditions.md)
  - [moveLeafs](../m/moveLeafs.md)
  - [doPickSplit](../d/doPickSplit.md)
  - [spgMatchNodeAction](spgMatchNodeAction.md)
  - [spgAddNodeAction](spgAddNodeAction.md)
  - [spgSplitNodeAction](spgSplitNodeAction.md)
  - [spgExtractNodeLabels](spgExtractNodeLabels.md)
- Called from (representative examples):
  - [spgistBuildCallback](spgistBuildCallback.md)
  - [spginsert](spginsert.md)

## Notes and Other Information
Returns true on successful insertion, false if insertion failed due to conflicts (requiring retry by caller). The function includes comprehensive interrupt handling to prevent infinite loops from broken opclasses, with progress tracking for tuple size reduction during prefix stripping. It manages buffer locking carefully to avoid deadlocks during tree descent, using conditional locking and retry mechanisms. The function supports both regular and null value insertion, routing to appropriate tree sections. Size validation prevents oversized tuples from being inserted unless the opclass supports long value handling through successive prefix stripping operations.