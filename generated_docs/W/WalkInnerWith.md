# WalkInnerWith

## Location
[src/backend/parser/parse_cte.c:812-862](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_cte.c#L812-L862)

## Overview
WalkInnerWith is a static helper function that handles the recursive traversal of statements containing WITH clauses, managing the visibility rules for Common Table Expression (CTE) names based on whether the WITH clause is recursive or non-recursive.

## Definition

```c
static void
WalkInnerWith(Node *stmt, WithClause *withClause, CteState *cstate)
```
## Detailed Description
This function is a subroutine of makeDependencyGraphWalker that specifically handles the complex visibility semantics of CTE names within WITH clauses. It implements two distinct behaviors:

**For RECURSIVE WITH clauses:**
- All CTE names in the WITH clause are visible to all WITH items as well as the main query
- Pushes all CTEs onto the innerwiths stack at once
- Processes all CTE queries, then the main statement
- Pops the entire CTE list from the stack

**For non-RECURSIVE WITH clauses:**  
- CTE names are only visible to WITH items that come after them and to the main query
- Processes CTEs sequentially, adding each CTE to the visibility list after processing its query
- This creates a progressive visibility scope where later CTEs can reference earlier ones

The function maintains the innerwiths list in CteState to track which CTEs are visible at each nesting level, ensuring proper dependency graph construction for CTE resolution.

## Parameters / Member Variables
- : The statement node that contains the WITH clause to be processed
- : The WITH clause containing the list of CTEs to process
- : The CTE state structure tracking dependency information and visibility scope

## Dependencies
- Functions called/Symbols referenced:
  - [lcons](../l/lcons.md) (list manipulation)
  - [makeDependencyGraphWalker](../m/makeDependencyGraphWalker.md) (recursive dependency analysis) 
  - raw_expression_tree_walker (generic AST traversal)
  - list_delete_first (list manipulation)
  - list_head (list access)
  - lappend (list manipulation)
  - CommonTableExpr (CTE structure)
  - WithClause (WITH clause structure)
  - [CteState](../C/CteState.md) (dependency tracking state)

- Called from:
  - [makeDependencyGraphWalker](../m/makeDependencyGraphWalker.md) (multiple call sites for different statement types)

## Notes and Other Information
- This function is critical for implementing SQL:1999 WITH clause semantics correctly
- The distinction between recursive and non-recursive WITH handling ensures proper CTE name resolution
- The innerwiths stack mechanism allows for proper nesting of WITH clauses
- The function uses raw_expression_tree_walker to ensure all sub-expressions are visited for dependency analysis
- Proper stack management (push/pop) is essential to maintain correct visibility scopes across nested WITH clauses