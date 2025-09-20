# rangeTableEntry_used_walker

## Location
[src/backend/rewrite/rewriteManip.c:900-966](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L900-L966)

## Overview
A tree walker function that determines whether a specific range table entry is referenced by any Var nodes, CurrentOfExpr nodes, RangeTblRef nodes, or JoinExpr nodes within a query tree or expression.

## Definition

```c
static bool
rangeTableEntry_used_walker(Node *node,
							rangeTableEntry_used_context *context)
```
## Detailed Description
This walker function recursively traverses a query tree or expression tree to detect references to a specific range table entry. It checks various node types that can reference range table entries:

- **Var nodes**: Checks if the variable references the target range table entry either directly (via varno) or through nulling relations (varnullingrels)
- **CurrentOfExpr nodes**: Detects cursor references to the target range table entry
- **RangeTblRef nodes**: Identifies direct references to the range table entry in FROM clauses
- **JoinExpr nodes**: Checks if join expressions reference the target range table entry

The function properly handles subquery nesting by tracking sublevels_up and only matching references at the appropriate query nesting level. When encountering Query nodes (subselects), it increments the sublevel counter, recurses into the subquery, then decrements the counter.

## Parameters / Member Variables
- : The current node being examined during tree traversal
- : Context structure containing:
  - : The range table index being searched for
  - : Current nesting level (0 = current query level)

## Dependencies
- Functions called/Symbols referenced:
  - rangeTableEntry_used_context (context structure)
  - [bms_is_member](../b/bms_is_member.md) (bitmap set membership check)
  - query_tree_walker (for recursing into subqueries)
  - expression_tree_walker (for traversing expression nodes)
  - Various node type checks: CurrentOfExpr, RangeTblRef, JoinExpr, PlaceHolderVar, etc.
- Called from (representative examples):
  - [rangeTableEntry_used_walker](rangeTableEntry_used_walker.md) (recursive calls)
  - [rangeTableEntry_used](rangeTableEntry_used.md) (main entry point)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:900-966
- Returns true immediately when a reference is found, false to continue searching
- Includes assertions to ensure planner auxiliary nodes are not encountered during rewrite phase
- Part of PostgreSQL's query rewriting infrastructure for detecting unused range table entries
- Handles both direct variable references and nulling relation references for outer join semantics
- Properly manages query nesting levels to avoid false matches across subquery boundaries