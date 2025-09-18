# contain_dml_walker

## Location
src/backend/optimizer/plan/subselect.c: 1062 - 1082

## Overview
A recursive tree walker function that traverses query trees to detect any Data Manipulation Language (DML) operations or row locking clauses.

## Definition
```c
static bool contain_dml_walker(Node *node, void *context)
```

## Detailed Description
This function implements the core logic for detecting non-SELECT operations in a query tree. It uses PostgreSQL's standard tree walker pattern to recursively examine all nodes in a query structure.

The function specifically checks for:

1. **Non-SELECT Commands**: Any command type other than CMD_SELECT (INSERT, UPDATE, DELETE, etc.)
2. **Row Locking**: Presence of row marks (FOR UPDATE, FOR SHARE clauses), indicated by a non-empty query->rowMarks list

When encountering a Query node, it first checks the command type and row marks. If these indicate DML or locking operations, it immediately returns true. Otherwise, it continues the recursive traversal using query_tree_walker for Query nodes and expression_tree_walker for other expression nodes.

The recursive nature ensures that nested subqueries are also examined, making it comprehensive for complex query structures.

## Parameters
- `node`: The current node in the tree traversal
- `context`: Walker context (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro)
  - query_tree_walker
  - expression_tree_walker
  - [contain_dml_walker](contain_dml_walker.md) (recursive self-reference)
- Called from (representative examples):
  - [contain_dml](contain_dml.md)
  - [contain_dml_walker](contain_dml_walker.md) (recursive calls)

## Notes and Other Information
- Follows PostgreSQL's standard tree walker pattern with recursive self-calls
- Returns true on first detection of DML/locking, providing early termination optimization
- Handles both Query nodes (with query_tree_walker) and expression nodes (with expression_tree_walker)
- The context parameter is unused but maintained for consistency with walker function signature
- Critical component in CTE inlining decisions to preserve side-effect semantics