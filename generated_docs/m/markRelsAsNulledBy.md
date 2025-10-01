# markRelsAsNulledBy

## Location
[src/backend/parser/parse_clause.c:1774-1814](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L1774-L1814)

## Overview
Recursively marks relations in a jointree node and its children as being nulled by a specific outer join.

## Definition
```c
static void markRelsAsNulledBy(ParseState *pstate, Node *n, int jindex)
```

## Detailed Description
This function implements a critical aspect of PostgreSQL's outer join semantics by tracking which relations can be nulled by outer joins. It recursively traverses a jointree node (either a RangeTblRef or JoinExpr) and marks all contained relations as being potentially nulled by the outer join identified by jindex. For RangeTblRef nodes, it directly extracts the rtindex; for JoinExpr nodes, it recursively processes both left and right arguments before handling the join's own rtindex. The function maintains the p_nullingrels list in the ParseState, which tracks which joins can null each relation. Since this list is maintained lazily, the function extends it as needed to accommodate the relation's varno. Finally, it adds the jindex to the appropriate Bitmapset using bms_add_member, creating a record that this relation can be nulled by the specified join. This information is essential for correct Var generation in subsequent query processing, ensuring that variables from nullable sides of outer joins are properly marked with their varnullingrels bitmaps.

## Parameters / Member Variables
- `pstate`: ParseState containing the p_nullingrels list to be updated
- `n`: Node representing the jointree element to process (RangeTblRef or JoinExpr)
- `jindex`: Integer index of the join that will null the relations in this subtree

## Dependencies
- Functions called/Symbols referenced:
  - [markRelsAsNulledBy](markRelsAsNulledBy.md) (recursive calls)
  - [bms_add_member](../b/bms_add_member.md)
  - [list_nth_cell](../l/list_nth_cell.md)
  - nodeTag
- Types referenced:
  - [RangeTblRef](../R/RangeTblRef.md)
  - [JoinExpr](../J/JoinExpr.md)
  - [Bitmapset](../B/Bitmapset.md)
- Called from (representative examples):
  - [transformFromClauseItem](../t/transformFromClauseItem.md) (for LEFT, RIGHT, and FULL outer joins)
  - [markRelsAsNulledBy](markRelsAsNulledBy.md) (recursive calls for nested joins)

## Notes and Other Information
- This is a static function within parse_clause.c used internally for outer join processing
- Essential for implementing correct outer join semantics in PostgreSQL
- The p_nullingrels list is maintained lazily and extended as needed
- Recursively handles nested join structures to ensure all affected relations are marked
- Critical for proper Var generation where varnullingrels must reflect nullability
- Only called for outer join types (LEFT, RIGHT, FULL) that can actually null relations
- The function processes the entire subtree to handle complex nested join scenarios

## Simplified Source

```c
static void
markRelsAsNulledBy(ParseState *pstate, Node *n, int jindex)
{
    int varno;

    // Handle different node types
    if (IsA(n, RangeTblRef))
    {
        varno = ((RangeTblRef *) n)->rtindex;
    }
    else if (IsA(n, JoinExpr))
    {
        JoinExpr *j = (JoinExpr *) n;

        // Recursively mark children
        markRelsAsNulledBy(pstate, j->larg, jindex);
        markRelsAsNulledBy(pstate, j->rarg, jindex);
        varno = j->rtindex;
    }
    else
    {
        elog(ERROR, "unrecognized node type: %d", (int) nodeTag(n));
        varno = 0;
    }

    // Extend p_nullingrels list if needed
    while (list_length(pstate->p_nullingrels) < varno)
        pstate->p_nullingrels = lappend(pstate->p_nullingrels, NULL);

    // Add jindex to the nulling relations bitmapset for this varno
    ListCell *lc = list_nth_cell(pstate->p_nullingrels, varno - 1);
    lfirst(lc) = bms_add_member((Bitmapset *) lfirst(lc), jindex);
}
```