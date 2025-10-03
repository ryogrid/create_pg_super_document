# transformValuesClause

## Location
[src/backend/parser/analyze.c:1480-1698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L1480-L1698)

## Overview
Transforms a VALUES clause used as a standalone SELECT statement into a Query tree, treating it as if it were "SELECT * FROM (VALUES ...) AS "*VALUES*"".

## Definition

```c
static Query *
transformValuesClause(ParseState *pstate, SelectStmt *stmt)
```
## Detailed Description
transformValuesClause handles the transformation of VALUES clauses that appear as standalone SELECT statements (not within INSERT or other contexts). The function creates a virtual range table entry (RTE) containing the VALUES data and builds a Query structure that selects from this RTE.

The transformation process involves several key steps: First, it validates that only VALUES-specific clauses are present (no FROM, WHERE, GROUP BY, etc.). Then it processes each row of VALUES data, transforming expressions and ensuring all rows have the same number of columns. The function performs type resolution to find common types across all rows in each column, coercing expressions to these common types. It also determines common type modifiers and collations for each column.

The intermediate representation is organized by columns rather than rows to simplify type processing, then reorganized back to row format for the final RTE. The function handles special cases like NEW/OLD references within CREATE RULE contexts by marking the RTE as LATERAL when necessary.

## Parameters / Member Variables
- `*pstate`: ParseState structure containing parsing context and namespace information
- `*stmt`: SelectStmt node representing the VALUES clause to be transformed
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (Query creation)
  - [transformWithClause](transformWithClause.md) (WITH clause processing)
  - [transformExpressionList](transformExpressionList.md) (expression transformation for each VALUES row)
  - [select_common_type](../s/select_common_type.md) (type resolution across columns)
  - [coerce_to_common_type](../c/coerce_to_common_type.md) (type coercion)
  - [select_common_typmod](../s/select_common_typmod.md)/select_common_collation (type modifier and collation resolution)
  - [contain_vars_of_level](../c/contain_vars_of_level.md) (variable reference detection)
  - [addRangeTableEntryForValues](../a/addRangeTableEntryForValues.md) (VALUES RTE creation)
  - [addNSItemToQuery](../a/addNSItemToQuery.md) (namespace item addition)
  - [expandNSItemAttrs](../e/expandNSItemAttrs.md) (target list generation)
  - [transformSortClause](transformSortClause.md)/transformLimitClause (ORDER BY and LIMIT processing)
  - [assign_query_collations](../a/assign_query_collations.md) (collation assignment)
- Called from (representative examples):
  - [transformStmt](transformStmt.md) (main statement transformation dispatcher)

## Notes and Other Information
- The function asserts that incompatible SELECT clauses (DISTINCT, INTO, FROM, WHERE, GROUP BY, HAVING, WINDOW) are not present
- All VALUES rows must have the same length after expression transformation (which may expand * expressions)
- Type resolution is performed column-wise to find common types, with all expressions in each column coerced to the common type
- The function supports ORDER BY and LIMIT clauses on VALUES but rejects FOR UPDATE/SHARE clauses
- LATERAL marking is applied when the VALUES expressions contain references to outer query variables (typically in CREATE RULE contexts)
- Memory optimization includes releasing intermediate sublists to save memory during processing
- The final Query structure appears as if selecting all columns from a virtual table containing the VALUES data

## Simplified Source

```c
static Query *
transformValuesClause(ParseState *pstate, SelectStmt *stmt)
{
    Query *qry = makeNode(Query);
    List *exprsLists = NIL;
    List *coltypes = NIL;
    List *coltypmods = NIL;
    List *colcollations = NIL;
    List **colexprs = NULL;
    int sublist_length = -1;
    bool lateral = false;
    ParseNamespaceItem *nsitem;

    qry->commandType = CMD_SELECT;

    // Process WITH clause
    if (stmt->withClause) {
        qry->hasRecursive = stmt->withClause->recursive;
        qry->cteList = transformWithClause(pstate, stmt->withClause);
        qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
    }

    // Transform each VALUES row and organize by columns
    foreach(lc, stmt->valuesLists) {
        List *sublist = (List *) lfirst(lc);

        // Transform expressions in this row
        sublist = transformExpressionList(pstate, sublist, EXPR_KIND_VALUES, false);

        // Ensure all rows have same length
        if (sublist_length < 0) {
            sublist_length = list_length(sublist);
            colexprs = (List **) palloc0(sublist_length * sizeof(List *));
        } else if (sublist_length != list_length(sublist)) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                           errmsg("VALUES lists must all be the same length")));
        }

        // Build per-column expression lists
        int i = 0;
        foreach(lc2, sublist) {
            Node *col = (Node *) lfirst(lc2);
            colexprs[i] = lappend(colexprs[i], col);
            i++;
        }

        list_free(sublist);
        exprsLists = lappend(exprsLists, NIL);
    }

    // Resolve common types for each column and coerce expressions
    for (int i = 0; i < sublist_length; i++) {
        Oid coltype = select_common_type(pstate, colexprs[i], "VALUES", NULL);

        // Coerce all expressions in this column to common type
        foreach(lc, colexprs[i]) {
            Node *col = coerce_to_common_type(pstate, (Node *) lfirst(lc), coltype, "VALUES");
            lfirst(lc) = (void *) col;
        }

        int32 coltypmod = select_common_typmod(pstate, colexprs[i], coltype);
        Oid colcoll = select_common_collation(pstate, colexprs[i], true);

        coltypes = lappend_oid(coltypes, coltype);
        coltypmods = lappend_int(coltypmods, coltypmod);
        colcollations = lappend_oid(colcollations, colcoll);
    }

    // Rearrange expressions back into row-organized lists
    for (int i = 0; i < sublist_length; i++) {
        forboth(lc, colexprs[i], lc2, exprsLists) {
            Node *col = (Node *) lfirst(lc);
            List *sublist = lfirst(lc2);
            sublist = lappend(sublist, col);
            lfirst(lc2) = sublist;
        }
        list_free(colexprs[i]);
    }

    // Check for variable references requiring LATERAL
    if (pstate->p_rtable != NIL && contain_vars_of_level((Node *) exprsLists, 0))
        lateral = true;

    // Create VALUES range table entry
    nsitem = addRangeTableEntryForValues(pstate, exprsLists, coltypes,
                                         coltypmods, colcollations, NULL, lateral, true);
    addNSItemToQuery(pstate, nsitem, true, true, true);

    // Generate target list and handle ORDER BY/LIMIT
    qry->targetList = expandNSItemAttrs(pstate, nsitem, 0, true, -1);
    qry->sortClause = transformSortClause(pstate, stmt->sortClause, &qry->targetList,
                                          EXPR_KIND_ORDER_BY, false);
    qry->limitOffset = transformLimitClause(pstate, stmt->limitOffset,
                                            EXPR_KIND_OFFSET, "OFFSET", stmt->limitOption);
    qry->limitCount = transformLimitClause(pstate, stmt->limitCount,
                                           EXPR_KIND_LIMIT, "LIMIT", stmt->limitOption);
    qry->limitOption = stmt->limitOption;

    // Assemble final query
    qry->rtable = pstate->p_rtable;
    qry->rteperminfos = pstate->p_rteperminfos;
    qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);
    qry->hasSubLinks = pstate->p_hasSubLinks;

    assign_query_collations(pstate, qry);
    return qry;
}
```