# ScanQueryWalker

## Location
[src/backend/utils/cache/plancache.c:1920-1948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1920-L1948)

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
  - [SubLink](SubLink.md)
  - [ScanQueryForLocks](ScanQueryForLocks.md)
  - expression_tree_walker
- Called from (representative examples):
  - [ScanQueryForLocks](ScanQueryForLocks.md)
  - [ScanQueryWalker](ScanQueryWalker.md) (recursive calls via expression_tree_walker)

## Notes and Other Information
- This is a callback function designed to work with PostgreSQL's expression_tree_walker framework
- The function explicitly avoids recursing into Query nodes to prevent double-processing
- It processes both the subselect within SubLinks and continues to process lefthand arguments
- Returns false to continue tree traversal (standard walker convention)
- The acquire parameter is passed as a pointer since it needs to be forwarded through the walker framework
- This function complements ScanQueryForLocks by handling SubLink subqueries that aren't in RTEs or CTEs

## Simplified Source

```c
static bool
ScanQueryWalker(Node *node, bool *acquire)
{
    if (node == NULL)
        return false;

    if (IsA(node, SubLink)) {
        SubLink *sub = (SubLink *) node;
        // Process the subquery for locks
        ScanQueryForLocks(castNode(Query, sub->subselect), *acquire);
        // Continue processing lefthand args
    }

    // Don't recurse into Query nodes - ScanQueryForLocks handles them
    return expression_tree_walker(node, ScanQueryWalker, (void *) acquire);
}
```