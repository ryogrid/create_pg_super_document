# walkStatEntryTree

## Location
src/backend/utils/adt/tsvector_op.c: 2489 - 2534

## Overview
Performs an in-order traversal of a binary tree containing TSVectorStat entries, returning one node at a time while maintaining traversal state using a stack-based approach.

## Definition


## Detailed Description
This function implements a stateful in-order traversal of a binary tree structure containing text search statistics. It uses a stack-based approach to maintain the current position in the tree across multiple function calls. The traversal follows the standard in-order pattern: left subtree, node itself, right subtree. The function handles three main cases: returning the current node if it has valid data (ndoc != 0), navigating to the right subtree and finding its leftmost node, or backtracking up the tree when a subtree is fully traversed.

The function works in conjunction with ts_setup_firstcall to provide iteration capability for set-returning functions that need to examine all entries in the statistics tree.

## Parameters / Member Variables
- `stat`: TSVectorStat structure containing the traversal stack, current stack position, tree root, and maximum depth information

## Dependencies
- Functions called/Symbols referenced:
  - [walkStatEntryTree](walkStatEntryTree.md) (recursive call)
  - Assert macro
- Called from (representative examples):
  - [ts_process_call](../t/ts_process_call.md)
  - [walkStatEntryTree](walkStatEntryTree.md) (recursive self-call)

## Notes and Other Information
- Implements iterative in-order tree traversal using an explicit stack instead of recursive calls
- Uses the ndoc field to determine if a node contains valid data worth returning
- Maintains stack position (stackpos) to track current location in traversal
- Handles three traversal states: current node processing, right subtree navigation, and parent backtracking
- Returns NULL when traversal is complete (stack position reaches 0 and no more nodes)
- The stack prevents stack overflow that could occur with purely recursive traversal of deep trees
- Part of PostgreSQL's text search statistics functionality for TSVector operations