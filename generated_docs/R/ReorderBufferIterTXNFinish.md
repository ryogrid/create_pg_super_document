# ReorderBufferIterTXNFinish

## Location
src/backend/replication/logical/reorderbuffer.c: 1500 - 1530

## Overview
Deallocates and cleans up the iterator state structure used for iterating over reorder buffer transactions, releasing associated resources including file descriptors and memory.

## Definition


## Detailed Description
This function performs cleanup operations for a transaction iterator state structure. It systematically releases all resources allocated during the iterator's lifetime, including:

1. **File descriptor cleanup**: Iterates through all transaction entries and closes any open virtual file descriptors (VFDs) that were used for reading serialized transaction data from disk
2. **Memory leak prevention**: Checks for any remaining changes in the old_change list that may have been "leaked" during the last iteration call and properly returns them to the reorder buffer's change pool
3. **Heap structure cleanup**: Frees the binary heap used for ordering transaction changes
4. **State structure cleanup**: Deallocates the iterator state structure itself

The function ensures proper resource management by preventing file descriptor leaks and memory leaks that could occur during transaction iteration operations.

## Parameters / Member Variables
- : Pointer to the main ReorderBuffer structure that manages the overall reordering operations
- : Pointer to the ReorderBufferIterTXNState structure containing the iterator's current state, including open files, heap structure, and change lists

## Dependencies
- Functions called/Symbols referenced:
  - FileClose
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - dlist_container
  - [dlist_pop_head_node](../d/dlist_pop_head_node.md)
  - [ReorderBufferReturnChange](ReorderBufferReturnChange.md)
  - [binaryheap_free](../b/binaryheap_free.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - CHANGES_THRESHOLD (multiple locations in reorderbuffer.c)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the reorderbuffer.c file
- The function includes an assertion to ensure the old_change list is empty after cleanup
- The cleanup is performed in a specific order: files first, then leaked changes, then heap structure, and finally the state structure itself
- The function handles the case where some transaction entries may not have open file descriptors (vfd == -1)
- This function is part of the logical replication infrastructure in PostgreSQL