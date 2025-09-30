# transformIndexStmt

## Location
[src/backend/parser/parse_utilcmd.c:2797-2891](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L2797-L2891)

## Overview
Performs parse analysis for CREATE INDEX statements and ALTER TABLE statements that involve index creation, transforming index expressions and predicate clauses into their final parsed form.

## Definition
IndexStmt *transformIndexStmt(Oid relid, IndexStmt *stmt, const char *queryString)

## Detailed Description
The transformIndexStmt function handles the parsing and transformation of CREATE INDEX and ALTER TABLE statements that create indexes. Its primary responsibilities include:

1. **Parse State Setup**: Creates a ParseState structure to manage the parsing context and associates it with the query string.

2. **Relation Handling**: Opens the target relation using the provided OID and adds it to the range table, enabling expression references to table columns without qualification.

3. **WHERE Clause Transformation**: If the index has a predicate (WHERE clause), it transforms and validates the predicate expression, ensuring proper collation assignment.

4. **Index Expression Processing**: For functional indexes, it processes index expressions by:
   - Generating column names for expressions if not explicitly provided
   - Transforming expressions using the parse state
   - Assigning proper collations to the transformed expressions

5. **Validation**: Ensures that index expressions and predicates only reference the table being indexed, preventing invalid cross-table references.

The function includes an optimization where it returns immediately if the statement has already been transformed, avoiding redundant processing. It's designed to be safe against race conditions by relying solely on the passed relid parameter rather than the statement's relation field.

## Parameters / Member Variables
- : Object identifier of the relation on which the index is being created
- : IndexStmt structure containing the parsed index definition that needs transformation
- : Original SQL query string used for error reporting and context

## Dependencies
- Functions called/Symbols referenced:
  - [make_parsestate](../m/make_parsestate.md)
  - [relation_open](../r/relation_open.md)
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md)
  - [addNSItemToQuery](../a/addNSItemToQuery.md)
  - [transformWhereClause](transformWhereClause.md)
  - [assign_expr_collations](../a/assign_expr_collations.md)
  - [FigureIndexColname](../F/FigureIndexColname.md)
  - [transformExpr](transformExpr.md)
  - [free_parsestate](../f/free_parsestate.md)
  - [table_close](table_close.md)
- Called from (representative examples):
  - [ATPostAlterTypeParse](../A/ATPostAlterTypeParse.md)
  - [transformAlterTableStmt](transformAlterTableStmt.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- The function is a no-op for simple indexes that don't use expressions or predicates
- Several code paths create indexes without calling this function when they know no expressions need processing
- The transformed flag prevents redundant processing of already-transformed statements
- Race condition safety is achieved by using the relid parameter instead of stmt->relation
- The function validates that expressions only reference the target table, though this check is noted as potentially dead code
- Proper collation handling is crucial for both WHERE clauses and index expressions

## Simplified Source

```c
IndexStmt *
transformIndexStmt(Oid relid, IndexStmt *stmt, const char *queryString)
{
    ParseState *pstate;
    ParseNamespaceItem *nsitem;
    ListCell *l;
    Relation rel;

    // Skip if already transformed
    if (stmt->transformed)
        return stmt;

    // Set up parse state for expression processing
    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;

    // Open target relation and add to parse context
    rel = relation_open(relid, NoLock);
    nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock, NULL, false, true);
    addNSItemToQuery(pstate, nsitem, false, true, true);

    // Transform WHERE clause (index predicate) if present
    if (stmt->whereClause) {
        stmt->whereClause = transformWhereClause(pstate, stmt->whereClause,
                                               EXPR_KIND_INDEX_PREDICATE, "WHERE");
        // Fix collations for the predicate
        assign_expr_collations(pstate, stmt->whereClause);
    }

    // Transform index expressions (for functional indexes)
    foreach(l, stmt->indexParams) {
        IndexElem *ielem = (IndexElem *) lfirst(l);

        if (ielem->expr) {
            // Generate column name if not provided
            if (ielem->indexcolname == NULL)
                ielem->indexcolname = FigureIndexColname(ielem->expr);

            // Transform the expression
            ielem->expr = transformExpr(pstate, ielem->expr, EXPR_KIND_INDEX_EXPRESSION);

            // Fix collations for the expression
            assign_expr_collations(pstate, ielem->expr);
        }
    }

    // Validate that only base relation is referenced
    if (list_length(pstate->p_rtable) != 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg("index expressions and predicates can refer only to the table being indexed")));

    // Clean up
    free_parsestate(pstate);
    table_close(rel, NoLock);

    // Mark as transformed to avoid reprocessing
    stmt->transformed = true;

    return stmt;
}
```