# transformGroupClause

## Location
[src/backend/parser/parse_clause.c:2632-2731](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L2632-L2731)

## Overview
Transforms a GROUP BY clause (or window PARTITION BY clause) into a flat list of SortGroupClause nodes while building the groupingSets tree structure.

## Definition

```c
List *
transformGroupClause(ParseState *pstate, List *grouplist, List **groupingSets,
					 List **targetlist, List *sortClause,
					 ParseExprKind exprKind, bool useSQL99)
```
## Detailed Description
This function processes GROUP BY clauses and window PARTITION BY clauses, handling both simple grouping and complex grouping sets (CUBE, ROLLUP, GROUPING SETS). It performs two main tasks: (1) creates a flat list of SortGroupClause nodes referencing each distinct grouping expression, adding them to the targetlist as resjunk columns if needed, and (2) builds the groupingSets tree using ressortgrouprefs stored in GroupingSet nodes. The function first flattens implicit RowExprs recursively, then processes each item in the flattened list. For GroupingSet nodes, it handles different kinds (EMPTY, SETS, CUBE, ROLLUP) appropriately, while simple expressions are transformed via transformGroupClauseExpr. The function maintains proper nesting constraints where GROUPING_SET_SETS can contain SIMPLE, CUBE, or ROLLUP nodes, but CUBE and ROLLUP can only contain SIMPLE nodes.

## Parameters / Member Variables
- `*pstate`: ParseState containing parsing context and state information
- `*grouplist`: Input clause to transform (GROUP BY or PARTITION BY expressions)
- `**groupingSets`: Reference to list that will contain the grouping set tree structure
- `**targetlist`: Reference to TargetEntry list where grouping expressions are added as resjunk
- `*sortClause`: ORDER BY clause containing SortGroupClause nodes for reference
- `exprKind`: ParseExprKind enum value specifying the type of expression being parsed
- `useSQL99`: Boolean flag indicating whether to use SQL99 syntax rather than SQL92 syntax
## Dependencies
- Functions called/Symbols referenced:
  - [flatten_grouping_sets](../f/flatten_grouping_sets.md)
  - [makeGroupingSet](../m/makeGroupingSet.md)
  - [transformGroupingSet](transformGroupingSet.md)
  - [transformGroupClauseExpr](transformGroupClauseExpr.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [exprLocation](../e/exprLocation.md)
  - list_make1_int
  - [GroupingSet](../G/GroupingSet.md) (struct type)
  - [ParseExprKind](../P/ParseExprKind.md) (enum type)
  - GROUPING_SET_EMPTY, GROUPING_SET_SIMPLE, GROUPING_SET_SETS, GROUPING_SET_CUBE, GROUPING_SET_ROLLUP (enum values)
- Called from (representative examples):
  - [transformSelectStmt](transformSelectStmt.md)
  - [transformPLAssignStmt](transformPLAssignStmt.md)
  - [transformWindowDefinitions](transformWindowDefinitions.md)

## Notes and Other Information
- This is a public function declared in parse_clause.h
- Handles both GROUP BY and window PARTITION BY clauses (PARTITION BY always uses SQL99 rules)
- Can result in an empty groupClause with non-empty groupingSets (e.g., GROUP BY ())
- Automatically adds grouping expressions to targetlist as resjunk columns if not already present
- Maintains a local bitmap to track seen expressions and avoid duplicates within the same grouping context
- The groupingSets tree uses integer ressortgrouprefs rather than actual expressions for efficiency
- Supports complex grouping operations including CUBE (with exponential expansion) and ROLLUP
- Part of PostgreSQL's advanced SQL standard compliance for GROUP BY operations

## Simplified Source

```c
List *
transformGroupClause(ParseState *pstate, List *grouplist, List **groupingSets,
                     List **targetlist, List *sortClause,
                     ParseExprKind exprKind, bool useSQL99)
{
    List *result = NIL;
    List *flat_grouplist;
    List *gsets = NIL;
    ListCell *gl;
    bool hasGroupingSets = false;
    Bitmapset *seen_local = NULL;

    // Recursively flatten implicit RowExprs
    flat_grouplist = (List *) flatten_grouping_sets((Node *) grouplist,
                                                     true,
                                                     &hasGroupingSets);

    // Handle empty list with grouping sets - restore canonical form
    if (flat_grouplist == NIL && hasGroupingSets) {
        flat_grouplist = list_make1(makeGroupingSet(GROUPING_SET_EMPTY,
                                                     NIL,
                                                     exprLocation((Node *) grouplist)));
    }

    foreach(gl, flat_grouplist) {
        Node *gexpr = (Node *) lfirst(gl);

        if (IsA(gexpr, GroupingSet)) {
            GroupingSet *gset = (GroupingSet *) gexpr;

            switch (gset->kind) {
                case GROUPING_SET_EMPTY:
                    gsets = lappend(gsets, gset);
                    break;
                case GROUPING_SET_SIMPLE:
                    Assert(false);  // can't happen
                    break;
                case GROUPING_SET_SETS:
                case GROUPING_SET_CUBE:
                case GROUPING_SET_ROLLUP:
                    gsets = lappend(gsets,
                                    transformGroupingSet(&result,
                                                         pstate, gset,
                                                         targetlist, sortClause,
                                                         exprKind, useSQL99, true));
                    break;
            }
        }
        else {
            // Transform regular grouping expression
            Index ref = transformGroupClauseExpr(&result, seen_local,
                                                  pstate, gexpr,
                                                  targetlist, sortClause,
                                                  exprKind, useSQL99, true);

            if (ref > 0) {
                seen_local = bms_add_member(seen_local, ref);
                if (hasGroupingSets)
                    gsets = lappend(gsets,
                                    makeGroupingSet(GROUPING_SET_SIMPLE,
                                                    list_make1_int(ref),
                                                    exprLocation(gexpr)));
            }
        }
    }

    Assert(gsets == NIL || groupingSets != NULL);

    if (groupingSets)
        *groupingSets = gsets;

    return result;
}
```