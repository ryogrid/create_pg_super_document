# ScanQueryWalker

## Location
src/backend/utils/cache/plancache.c: 1920 - 1948

## Overview
A tree walker function that finds SubLink subqueries within expression trees and delegates their lock processing to ScanQueryForLocks.

## Definition
```c
static bool ScanQueryWalker(Node *node, bool *acquire)
```

## Detailed Description
This function serves as a specialized tree walker designed to traverse expression trees and identify SubLink nodes that contain embedded subqueries. When a SubLink is found, it extracts the subselect query and delegates the lock processing to ScanQueryForLocks. The function uses expression_tree_walker to recursively traverse the expression tree while avoiding double-processing of Query nodes (since ScanQueryForLocks handles those directly). This walker is specifically designed to work with the PostgreSQL query tree walker framework and handles the lefthand arguments of SubLinks as well.

## Parameters / Member Variables
- `node`: Current node being processed in the expression tree traversal
- `acquire`: Pointer to boolean flag indicating whether to acquire locks (true) or release them (false)

## Dependencies
- Functions called/Symbols referenced:
  - SubLink
  - ScanQueryForLocks
  - expression_tree_walker
- Called from (representative examples):
  - ScanQueryForLocks
  - ScanQueryWalker (recursive calls via expression_tree_walker)

## Notes and Other Information
- This is a callback function designed to work with PostgreSQL's expression_tree_walker framework
- The function explicitly avoids recursing into Query nodes to prevent double-processing
- It processes both the subselect within SubLinks and continues to process lefthand arguments
- Returns false to continue tree traversal (standard walker convention)
- The acquire parameter is passed as a pointer since it needs to be forwarded through the walker framework
- This function complements ScanQueryForLocks by handling SubLink subqueries that aren't in RTEs or CTEs