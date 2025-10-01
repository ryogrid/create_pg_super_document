# addRangeTableEntryForSubquery

## Location
[src/backend/parser/parse_relation.c:1638-1733](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L1638-L1733)

## Overview
Creates a range table entry for a subquery and adds it to the parser state, returning a ParseNamespaceItem with appropriate column type information and visibility settings.

## Definition

```c
ParseNamespaceItem *
addRangeTableEntryForSubquery(ParseState *pstate,
							  Query *subquery,
							  Alias *alias,
							  bool lateral,
							  bool inFromCl)
```
## Detailed Description
The  function creates a range table entry for subqueries appearing in FROM clauses, WITH clauses, or other contexts where a query result is treated as a relation. This function handles the complex process of:

1. Creating an RTE with type RTE_SUBQUERY
2. Managing column aliases - either from user-provided aliases or auto-generated from subquery target list
3. Extracting type information (data types, type modifiers, collations) from the subquery's target list
4. Validating that the number of specified aliases matches available columns
5. Setting visibility rules based on whether the subquery has a user-provided alias

Key behavior:
- If no alias is provided, creates an auto-generated name "unnamed_subquery" that is marked as not visible
- Non-visible subqueries only allow unqualified column references and won't conflict with other namespace entries
- No permission checking is performed on subqueries since they represent derived data
- Extracts column metadata from the subquery's target list, skipping junk columns

## Parameters / Member Variables
- : Parser state containing the range table and other parsing context
- : The Query node representing the subquery to be added as an RTE
- : Optional alias with column names; if NULL, auto-generates names from subquery
- : Boolean indicating whether this is a LATERAL subquery with access to preceding FROM items
- : Boolean indicating whether this entry originates from a FROM clause

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for RTE creation)
  - copyObject (for alias copying)
  - [makeAlias](../m/makeAlias.md) (for auto-generated aliases)
  - [makeString](../m/makeString.md) (for column name creation)
  - [exprType](../e/exprType.md), exprTypmod, exprCollation (type information extraction)
  - [lappend](../l/lappend.md), lappend_oid, lappend_int (list manipulation)
  - [buildNSItemFromLists](../b/buildNSItemFromLists.md) (namespace item creation)
  - ereport (error reporting)
- Called from (representative examples):
  - [transformRangeSubselect](../t/transformRangeSubselect.md) (in parse_clause.c)
  - [transformInsertStmt](../t/transformInsertStmt.md) (in analyze.c)
  - [transformSetOperationTree](../t/transformSetOperationTree.md) (in analyze.c)
  - [convert_ANY_sublink_to_join](../c/convert_ANY_sublink_to_join.md) (in subselect.c)

## Notes and Other Information
- Subqueries are never checked for access rights since they represent derived data, not base relations
- Column alias validation ensures the number of provided aliases matches the number of non-junk columns
- Auto-generated subquery names ("unnamed_subquery") are marked as not visible to prevent namespace conflicts
- The function carefully extracts type information from the subquery's target list to ensure proper column metadata
- LATERAL subqueries have special scoping rules allowing them to reference columns from preceding FROM items
- Error handling includes detailed messages when alias count mismatches occur

## Simplified Source

```c
ParseNamespaceItem *
addRangeTableEntryForSubquery(ParseState *pstate, Query *subquery,
                              Alias *alias, bool lateral, bool inFromCl)
{
    RangeTblEntry *rte = makeNode(RangeTblEntry);
    Alias *eref;
    int numaliases;
    List *coltypes, *coltypmods, *colcollations;
    int varattno;
    ListCell *tlistitem;

    // Initialize the range table entry for subquery
    rte->rtekind = RTE_SUBQUERY;
    rte->subquery = subquery;
    rte->alias = alias;

    // Create effective reference name - use provided alias or auto-generate
    eref = alias ? copyObject(alias) : makeAlias("unnamed_subquery", NIL);
    numaliases = list_length(eref->colnames);

    // Extract column information from subquery target list
    coltypes = coltypmods = colcollations = NIL;
    varattno = 0;

    foreach(tlistitem, subquery->targetList)
    {
        TargetEntry *te = (TargetEntry *) lfirst(tlistitem);

        // Skip junk columns (not part of final result)
        if (te->resjunk)
            continue;

        varattno++;
        Assert(varattno == te->resno);

        // Auto-generate column names if not enough aliases provided
        if (varattno > numaliases)
        {
            char *attrname = pstrdup(te->resname);
            eref->colnames = lappend(eref->colnames, makeString(attrname));
        }

        // Extract type information for each column
        coltypes = lappend_oid(coltypes, exprType((Node *) te->expr));
        coltypmods = lappend_int(coltypmods, exprTypmod((Node *) te->expr));
        colcollations = lappend_oid(colcollations, exprCollation((Node *) te->expr));
    }

    // Validate alias count matches available columns
    if (varattno < numaliases)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                 errmsg("table \"%s\" has %d columns available but %d columns specified",
                        eref->aliasname, varattno, numaliases)));

    // Complete RTE setup
    rte->eref = eref;
    rte->lateral = lateral;
    rte->inFromCl = inFromCl;

    // Add RTE to parser state's range table
    pstate->p_rtable = lappend(pstate->p_rtable, rte);

    // Build namespace item with column metadata
    ParseNamespaceItem *nsitem = buildNSItemFromLists(rte, list_length(pstate->p_rtable),
                                                       coltypes, coltypmods, colcollations);

    // Set visibility - only visible as relation name if user provided alias
    nsitem->p_rel_visible = (alias != NULL);

    return nsitem;
}
```