# dofindsubquery

## Location
src/backend/utils/adt/tsquery_rewrite.c: 206 - 266

## Overview
The `dofindsubquery` function is the recursive core of the TSQuery rewriting mechanism, responsible for traversing the query tree and applying pattern matching and replacement operations throughout the entire tree structure.

## Definition
```c
static QTNode *dofindsubquery(QTNode *root, QTNode *ex, QTNode *subs, bool *isfind)
```

## Detailed Description
This function performs a recursive depth-first traversal of a TSQuery tree, attempting to match and replace patterns at each node. It first tries to match the pattern at the current root node using `findeq`, and if no match is found there, it recursively processes all child nodes. The function implements crucial tree simplification logic to handle cases where replacements result in void subtrees or nodes with insufficient children.

Key responsibilities include:
- Coordinating pattern matching across the entire query tree
- Managing recursive traversal with stack overflow protection
- Handling NULL subtree elimination after replacements
- Simplifying operator nodes that become invalid after child removal
- Preserving tree structure integrity through proper node management

The function includes safeguards against stack overflow and supports query cancellation for long-running operations.

## Parameters / Member Variables
- `root`: The root node of the current subtree being processed
- `ex`: The example/pattern node to search for and match
- `subs`: The substitution node to replace matched patterns (can be NULL for deletion)
- `isfind`: Output parameter set to true if any replacement was made in this subtree

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - CHECK_FOR_INTERRUPTS (query cancellation support)
  - findeq (pattern matching at current node)
  - QTNFree (memory cleanup for removed nodes)
  - dofindsubquery (recursive self-call)
- Called from (representative examples):
  - findsubquery
  - dofindsubquery (recursive calls)

## Notes and Other Information
- Implements recursive tree traversal with proper stack depth checking to prevent overflow
- Automatically simplifies tree structure by removing nodes with zero children and collapsing single-child operator nodes (except OP_NOT)
- The function is designed to handle the complete tree transformation process, ensuring structural integrity after pattern replacements
- Uses the QTN_NOCHANGE flag to optimize traversal by skipping already-processed nodes
- Properly manages memory by freeing nodes that become unnecessary after simplification
- Supports interruption for long-running operations through CHECK_FOR_INTERRUPTS calls