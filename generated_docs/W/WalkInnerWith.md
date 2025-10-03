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
- `*stmt`: The statement node that contains the WITH clause to be processed
- `*withClause`: The WITH clause containing the list of CTEs to process
- `*cstate`: The CTE state structure tracking dependency information and visibility scope
## Dependencies
- Functions called/Symbols referenced:
  - [lcons](../l/lcons.md) (list manipulation)
  - [makeDependencyGraphWalker](../m/makeDependencyGraphWalker.md) (recursive dependency analysis) 
  - raw_expression_tree_walker (generic AST traversal)
  - [list_delete_first](../l/list_delete_first.md) (list manipulation)
  - [list_head](../l/list_head.md) (list access)
  - [lappend](../l/lappend.md) (list manipulation)
  - CommonTableExpr (CTE structure)
  - [WithClause](WithClause.md) (WITH clause structure)
  - [CteState](../C/CteState.md) (dependency tracking state)

- Called from:
  - [makeDependencyGraphWalker](../m/makeDependencyGraphWalker.md) (multiple call sites for different statement types)

## Notes and Other Information
- This function is critical for implementing SQL:1999 WITH clause semantics correctly
- The distinction between recursive and non-recursive WITH handling ensures proper CTE name resolution
- The innerwiths stack mechanism allows for proper nesting of WITH clauses
- The function uses raw_expression_tree_walker to ensure all sub-expressions are visited for dependency analysis
- Proper stack management (push/pop) is essential to maintain correct visibility scopes across nested WITH clauses

## Simplified Source

```c
static void
WalkInnerWith(Node *stmt, WithClause *withClause, CteState *cstate)
{
    if (withClause->recursive) {
        // Recursive WITH: all CTEs visible to all items and main query
        cstate->innerwiths = lcons(withClause->ctes, cstate->innerwiths);

        // Process all CTE queries
        foreach(lc, withClause->ctes) {
            CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
            makeDependencyGraphWalker(cte->ctequery, cstate);
        }

        // Process main statement
        raw_expression_tree_walker(stmt, makeDependencyGraphWalker, cstate);

        // Clean up visibility stack
        cstate->innerwiths = list_delete_first(cstate->innerwiths);
    } else {
        // Non-recursive WITH: sequential visibility (later CTEs see earlier ones)
        cstate->innerwiths = lcons(NIL, cstate->innerwiths);

        foreach(lc, withClause->ctes) {
            CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

            // Process CTE query first
            makeDependencyGraphWalker(cte->ctequery, cstate);

            // Add this CTE to visibility list for subsequent CTEs
            ListCell *cell1 = list_head(cstate->innerwiths);
            lfirst(cell1) = lappend((List *) lfirst(cell1), cte);
        }

        // Process main statement
        raw_expression_tree_walker(stmt, makeDependencyGraphWalker, cstate);

        // Clean up visibility stack
        cstate->innerwiths = list_delete_first(cstate->innerwiths);
    }
}
```