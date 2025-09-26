# checkExprHasSubLink_walker

## Location
[src/backend/rewrite/rewriteManip.c:309-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L309-L324)

## Overview
A static walker function that recursively traverses expression trees to detect the presence of SubLink nodes representing subqueries within expressions.

## Definition
```c
static bool checkExprHasSubLink_walker(Node *node, void *context)
```

## Detailed Description
This function implements the core logic for detecting SubLink nodes within expression trees. It follows the standard PostgreSQL walker pattern, performing a depth-first traversal of the expression tree and checking each node to determine if it represents a SubLink node. SubLink nodes represent various types of subqueries that appear within expressions, such as EXISTS clauses, IN/ANY/ALL subqueries, and scalar subqueries. The function is designed to stop traversal immediately upon finding any SubLink, returning true to indicate its presence. Unlike some other walker functions in this module, this walker does not explicitly avoid subselects, as the traversal control is handled by the calling function through query_or_expression_tree_walker flags.

## Parameters / Member Variables
- `node`: The current node being examined in the expression tree traversal
- `context`: A void pointer to walker context (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [SubLink](../S/SubLink.md) (node type check)
  - expression_tree_walker (recursive traversal)
  - [checkExprHasSubLink_walker](checkExprHasSubLink_walker.md) (recursive self-call)
- Called from (representative examples):
  - [checkExprHasSubLink](checkExprHasSubLink.md)
  - expression_tree_walker (during recursive traversal)

## Notes and Other Information
- This is a static function, only accessible within src/backend/rewrite/rewriteManip.c
- Returns true immediately upon finding the first SubLink node, making it an efficient short-circuit search
- The context parameter is not used but maintained for compatibility with the walker function signature pattern
- Part of the query analysis infrastructure used to determine when special handling is needed for subqueries
- Used extensively in query rewriting contexts where the presence of subqueries affects processing logic
- The traversal behavior is controlled by the caller through query_or_expression_tree_walker flags