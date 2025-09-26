# contain_windowfuncs_walker

## Location
[src/backend/rewrite/rewriteManip.c:229-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L229-L253)

## Overview
A static walker function that recursively traverses expression trees to detect the presence of window function calls at the current query level.

## Definition
```c
static bool contain_windowfuncs_walker(Node *node, void *context)
```

## Detailed Description
This function is a tree walker that implements the core logic for detecting window function calls within expression trees. It follows the standard PostgreSQL walker pattern, performing a depth-first traversal of the expression tree and checking each node to determine if it represents a WindowFunc node. The function is designed to stop traversal immediately upon finding a window function, returning true to indicate its presence. It uses the expression_tree_walker infrastructure to recursively visit all nodes in the expression tree while avoiding descent into subselects, which would represent different query levels.

## Parameters / Member Variables
- `node`: The current node being examined in the expression tree traversal
- `context`: A void pointer to walker context (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [WindowFunc](../W/WindowFunc.md) (node type check)
  - expression_tree_walker (recursive traversal)
  - [contain_windowfuncs_walker](contain_windowfuncs_walker.md) (recursive self-call)
- Called from (representative examples):
  - [contain_windowfuncs](contain_windowfuncs.md)
  - expression_tree_walker (during recursive traversal)

## Notes and Other Information
- This is a static function, only accessible within src/backend/rewrite/rewriteManip.c
- The function explicitly avoids recursing into subselects to maintain proper query level isolation
- Returns true immediately upon finding the first WindowFunc node, making it an efficient short-circuit search
- Part of the query rewrite system's expression analysis capabilities
- The context parameter is not used but maintained for compatibility with the walker function signature pattern