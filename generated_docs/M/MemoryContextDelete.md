# MemoryContextDelete

## Location
src/backend/utils/mmgr/mcxt.c: 454 - 495

## Overview
Deletes a context and its descendants, releasing all space allocated therein, using a bottom-up traversal to avoid recursion.

## Definition


## Detailed Description
MemoryContextDelete is a comprehensive memory management function that completely removes a memory context and all its descendant contexts from the memory hierarchy. Unlike reset operations, this function permanently destroys the contexts and frees all associated memory.

The function employs a sophisticated bottom-up traversal strategy specifically designed to avoid recursion. This is crucial for PostgreSQL's robustness, as a "stack depth limit exceeded" error would be catastrophic during transaction cleanup. The algorithm descends to find leaf contexts (those with no children), deletes them using MemoryContextDeleteOnly(), then moves up to their parents, repeating until the original context is deleted.

The implementation avoids using MemoryContextTraverseNext() because it modifies the tree structure during traversal, which would interfere with standard tree traversal algorithms.

## Parameters / Member Variables
- : The memory context to delete along with all its descendants. Must be a valid MemoryContext.

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextIsValid
  - [MemoryContextDeleteOnly](MemoryContextDeleteOnly.md)
- Called from (representative examples):
  - [brininsert](../b/brininsert.md)
  - [FreeExecutorState](../F/FreeExecutorState.md)
  - SPI_finish
  - [AtCommit_Memory](../A/AtCommit_Memory.md)
  - PortalDrop

## Notes and Other Information
- The function includes an assertion to validate that the input context is valid before proceeding
- Uses an iterative approach instead of recursion to prevent stack overflow during error recovery scenarios
- The traversal modifies the tree structure as it progresses, requiring a specialized algorithm
- This is a destructive operation - once called, the context and all its descendants are permanently removed
- Widely used throughout PostgreSQL for cleanup operations in executors, SPI, transaction management, and various subsystems
- The bottom-up approach ensures that parent contexts are only deleted after all their children have been properly cleaned up