# locate_windowfunc_walker

## Location
[src/backend/rewrite/rewriteManip.c:273-295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L273-L295)

## Overview
A static walker function that traverses expression trees to find the first window function with a known parse location, storing that location in the provided context.

## Definition
```c
static bool locate_windowfunc_walker(Node *node, locate_windowfunc_context *context)
```

## Detailed Description
This function implements the core logic for locating window function parse locations within expression trees. It follows the standard PostgreSQL walker pattern, performing a depth-first traversal while checking each node to see if it's a WindowFunc with a valid location field (>= 0). When it finds a WindowFunc with a known location, it stores that location in the context structure and returns true to abort further tree traversal. If a WindowFunc is found but has no location information, the function continues traversing to look for other window functions that might have location data. Like other walker functions in this module, it avoids recursing into subselects to maintain proper query level isolation.

## Parameters / Member Variables
- `node`: The current node being examined in the expression tree traversal
- `context`: Pointer to locate_windowfunc_context structure containing the win_location field to store the found location

## Dependencies
- Functions called/Symbols referenced:
  - [locate_windowfunc_context](locate_windowfunc_context.md) (context structure type)
  - [WindowFunc](../W/WindowFunc.md) (node type and location field access)
  - expression_tree_walker (recursive traversal)
  - [locate_windowfunc_walker](locate_windowfunc_walker.md) (recursive self-call)
- Called from (representative examples):
  - [locate_windowfunc](locate_windowfunc.md)
  - expression_tree_walker (during recursive traversal)

## Notes and Other Information
- This is a static function, only accessible within src/backend/rewrite/rewriteManip.c
- Returns true immediately upon finding the first WindowFunc with a valid location (>= 0)
- Continues searching if a WindowFunc is found but has no location information
- The function explicitly avoids recursing into subselects to maintain proper query level isolation
- Part of the error reporting infrastructure, used to provide accurate source locations in error messages
- The context structure is modified in-place to return the found location to the caller