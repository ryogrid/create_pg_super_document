# assign_query_collations_walker

## Location
[src/backend/parser/parse_collate.c:118-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_collate.c#L118-L143)

## Overview
Walker function for assign_query_collations that processes individual expressions to assign appropriate collations while respecting boundaries of set operations.

## Definition
```c
static bool assign_query_collations_walker(Node *node, ParseState *pstate)
```

## Detailed Description
This function serves as the walker callback for query_tree_walker during collation assignment. It processes each expression found by the walker independently and ensures that different parts of the query tree (like different target entries) are handled separately rather than attempting to derive a common collation across unrelated expressions.

The function specifically avoids recursing into SetOperationStmt nodes since they have already been fully processed during transformSetOperationStmt and have their collations properly established.

## Parameters / Member Variables
- `node`: The node being processed (may be NULL, a List, or individual expression)
- `pstate`: Parser state providing context for collation assignment

## Dependencies
- Functions called/Symbols referenced:
  - assign_list_collations (for List nodes)
  - assign_expr_collations (for individual expressions)
  - IsA (type checking)
- Called from:
  - assign_query_collations via query_tree_walker

## Notes and Other Information
- Each expression is processed independently to avoid inappropriate collation conflicts
- SetOperationStmt nodes are skipped since they're already processed
- Part of PostgreSQL's comprehensive collation assignment system

## Simplified Source

```c
static bool
assign_query_collations_walker(Node *node, ParseState *pstate)
{
    // Skip empty nodes
    if (node == NULL)
        return false;

    // Don't recurse into already-processed set operations
    if (IsA(node, SetOperationStmt))
        return false;

    // Process lists and expressions independently
    if (IsA(node, List))
        assign_list_collations(pstate, (List *) node);
    else
        assign_expr_collations(pstate, node);

    return false;
}
```