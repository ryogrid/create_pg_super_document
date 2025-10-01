# transformFromClauseItem

## Location
[src/backend/parser/parse_clause.c:1056-1639](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L1056-L1639)

## Overview
Transforms a FROM-clause item into a processed node for the join tree, handling various relation types including tables, subselects, functions, and joins.

## Definition
```c
static Node *transformFromClauseItem(ParseState *pstate, Node *n,
                                   ParseNamespaceItem **top_nsitem,
                                   List **namespace)
```

## Detailed Description
This is a central recursive function in PostgreSQL's FROM clause processing that transforms raw parse tree nodes into processed joinlist nodes while building namespace information. It handles multiple FROM clause item types: RangeVar (table references, CTEs, ENRs), RangeSubselect (subqueries), RangeFunction (function calls), RangeTableFunc/JsonTable (table functions), RangeTableSample (TABLESAMPLE clauses), and JoinExpr (JOIN operations). For simple relations, it creates RangeTblRef nodes after resolving the relation through appropriate transform functions. For joins, it performs complex processing including recursive transformation of left and right arguments, handling of LATERAL references, NATURAL JOIN column matching, USING clause processing, ON clause transformation, outer join nullability marking via markRelsAsNulledBy, and construction of merged column variables using buildMergedJoinVar and buildVarFromNSColumn. The function maintains careful namespace management to ensure proper column visibility and conflict resolution throughout the transformation process.

## Parameters / Member Variables
- `pstate`: ParseState containing the current parsing context and range table
- `n`: Input Node representing the FROM clause item to be transformed
- `top_nsitem`: Output parameter receiving the ParseNamespaceItem for the transformed item
- `namespace`: Output parameter receiving the list of ParseNamespaceItems exposed by this item

## Dependencies
- Functions called/Symbols referenced:
  - [getNSItemForSpecialRelationTypes](../g/getNSItemForSpecialRelationTypes.md)
  - [transformTableEntry](transformTableEntry.md)
  - [transformRangeSubselect](transformRangeSubselect.md)
  - [transformRangeFunction](transformRangeFunction.md)
  - [transformJsonTable](transformJsonTable.md)
  - [transformRangeTableFunc](transformRangeTableFunc.md)
  - [transformRangeTableSample](transformRangeTableSample.md)
  - [buildVarFromNSColumn](../b/buildVarFromNSColumn.md)
  - [buildMergedJoinVar](../b/buildMergedJoinVar.md)
  - [markRelsAsNulledBy](../m/markRelsAsNulledBy.md)
  - [addRangeTableEntryForJoin](../a/addRangeTableEntryForJoin.md)
  - [checkNameSpaceConflicts](../c/checkNameSpaceConflicts.md)
  - [setNamespaceLateralState](../s/setNamespaceLateralState.md)
  - [transformJoinUsingClause](transformJoinUsingClause.md)
  - [transformJoinOnClause](transformJoinOnClause.md)
- Called from (representative examples):
  - [transformFromClause](transformFromClause.md) (main entry point)
  - [transformFromClauseItem](transformFromClauseItem.md) (recursive calls for join processing)

## Notes and Other Information
- This is a static function within parse_clause.c used internally for FROM clause processing
- Supports stack depth checking to prevent infinite recursion in deeply nested structures
- Handles LATERAL reference visibility by temporarily modifying the parse state's namespace
- Implements SQL standard rules for NATURAL JOIN column matching and USING clause processing
- Manages outer join nullability marking to ensure correct Var generation for nullable columns
- The function can recursively call itself when processing JOIN expressions
- Critical for proper namespace construction and column visibility in complex FROM clauses

## Simplified Source

```c
static Node *
transformFromClauseItem(ParseState *pstate, Node *n,
                       ParseNamespaceItem **top_nsitem,
                       List **namespace)
{
    check_stack_depth();

    if (IsA(n, RangeVar)) {
        // Handle table references and CTEs
        RangeVar *rv = (RangeVar *) n;
        ParseNamespaceItem *nsitem;

        // Check for CTE or special relation types first
        nsitem = getNSItemForSpecialRelationTypes(pstate, rv);
        if (!nsitem)
            nsitem = transformTableEntry(pstate, rv);

        *top_nsitem = nsitem;
        *namespace = list_make1(nsitem);

        // Create range table reference
        RangeTblRef *rtr = makeNode(RangeTblRef);
        rtr->rtindex = nsitem->p_rtindex;
        return (Node *) rtr;
    }
    else if (IsA(n, RangeSubselect)) {
        // Handle subqueries
        ParseNamespaceItem *nsitem = transformRangeSubselect(pstate, (RangeSubselect *) n);
        *top_nsitem = nsitem;
        *namespace = list_make1(nsitem);

        RangeTblRef *rtr = makeNode(RangeTblRef);
        rtr->rtindex = nsitem->p_rtindex;
        return (Node *) rtr;
    }
    else if (IsA(n, RangeFunction)) {
        // Handle function calls
        ParseNamespaceItem *nsitem = transformRangeFunction(pstate, (RangeFunction *) n);
        *top_nsitem = nsitem;
        *namespace = list_make1(nsitem);

        RangeTblRef *rtr = makeNode(RangeTblRef);
        rtr->rtindex = nsitem->p_rtindex;
        return (Node *) rtr;
    }
    else if (IsA(n, RangeTableFunc) || IsA(n, JsonTable)) {
        // Handle table functions and JSON_TABLE
        ParseNamespaceItem *nsitem;
        if (IsA(n, JsonTable))
            nsitem = transformJsonTable(pstate, (JsonTable *) n);
        else
            nsitem = transformRangeTableFunc(pstate, (RangeTableFunc *) n);

        *top_nsitem = nsitem;
        *namespace = list_make1(nsitem);

        RangeTblRef *rtr = makeNode(RangeTblRef);
        rtr->rtindex = nsitem->p_rtindex;
        return (Node *) rtr;
    }
    else if (IsA(n, RangeTableSample)) {
        // Handle TABLESAMPLE clause
        RangeTableSample *rts = (RangeTableSample *) n;
        Node *rel = transformFromClauseItem(pstate, rts->relation, top_nsitem, namespace);
        RangeTblEntry *rte = (*top_nsitem)->p_rte;

        // Validate TABLESAMPLE is applied to appropriate relation types
        if (rte->rtekind != RTE_RELATION ||
            (rte->relkind != RELKIND_RELATION &&
             rte->relkind != RELKIND_MATVIEW &&
             rte->relkind != RELKIND_PARTITIONED_TABLE)) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                          errmsg("TABLESAMPLE clause can only be applied to tables and materialized views")));
        }

        rte->tablesample = transformRangeTableSample(pstate, rts);
        return rel;
    }
    else if (IsA(n, JoinExpr)) {
        // Handle JOIN expressions
        JoinExpr *j = (JoinExpr *) n;
        ParseNamespaceItem *l_nsitem, *r_nsitem;
        List *l_namespace, *r_namespace;

        // Transform left side first
        j->larg = transformFromClauseItem(pstate, j->larg, &l_nsitem, &l_namespace);

        // Make left side available for LATERAL references in right side
        bool lateral_ok = (j->jointype == JOIN_INNER || j->jointype == JOIN_LEFT);
        setNamespaceLateralState(l_namespace, true, lateral_ok);
        int sv_namespace_length = list_length(pstate->p_namespace);
        pstate->p_namespace = list_concat(pstate->p_namespace, l_namespace);

        // Transform right side
        j->rarg = transformFromClauseItem(pstate, j->rarg, &r_nsitem, &r_namespace);

        // Restore namespace
        pstate->p_namespace = list_truncate(pstate->p_namespace, sv_namespace_length);

        // Check for namespace conflicts
        checkNameSpaceConflicts(pstate, l_namespace, r_namespace);

        // Handle NATURAL JOIN by generating USING clause
        if (j->isNatural) {
            List *natural_cols = NIL;
            // Find matching column names between left and right sides
            foreach(lx, l_nsitem->p_names->colnames) {
                char *l_colname = strVal(lfirst(lx));
                if (l_colname[0] == '\0') continue; // skip dropped columns

                foreach(rx, r_nsitem->p_names->colnames) {
                    char *r_colname = strVal(lfirst(rx));
                    if (strcmp(l_colname, r_colname) == 0) {
                        natural_cols = lappend(natural_cols, makeString(l_colname));
                        break;
                    }
                }
            }
            j->usingClause = natural_cols;
        }

        // Transform join conditions
        if (j->usingClause) {
            // Transform USING clause into ON condition
            j->quals = transformJoinUsingClause(pstate, /* build var lists from USING */);
        } else if (j->quals) {
            // Transform explicit ON condition
            j->quals = transformJoinOnClause(pstate, j, list_concat(l_namespace, r_namespace));
        }

        // Mark nullable relations for outer joins
        j->rtindex = list_length(pstate->p_rtable) + 1;
        switch (j->jointype) {
            case JOIN_LEFT:
                markRelsAsNulledBy(pstate, j->rarg, j->rtindex);
                break;
            case JOIN_RIGHT:
                markRelsAsNulledBy(pstate, j->larg, j->rtindex);
                break;
            case JOIN_FULL:
                markRelsAsNulledBy(pstate, j->larg, j->rtindex);
                markRelsAsNulledBy(pstate, j->rarg, j->rtindex);
                break;
        }

        // Build join result namespace and RTE
        ParseNamespaceItem *nsitem = addRangeTableEntryForJoin(pstate, /* join column info */);

        *top_nsitem = nsitem;
        *namespace = /* combined namespace with proper visibility */;

        return (Node *) j;
    }
    else {
        elog(ERROR, "unrecognized node type: %d", (int) nodeTag(n));
    }

    return NULL;
}
```