# transformStatsStmt

## Location
[src/backend/parser/parse_utilcmd.c:2892-2966](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L2892-L2966)

## Overview
Performs parse analysis for CREATE STATISTICS statements, transforming statistics expressions into their final parsed form for extended statistics objects.

## Definition
CreateStatsStmt *transformStatsStmt(Oid relid, CreateStatsStmt *stmt, const char *queryString)

## Detailed Description
The transformStatsStmt function handles the parsing and transformation of CREATE STATISTICS statements. Its primary responsibilities include:

1. **Parse State Setup**: Creates a ParseState structure to manage the parsing context and associates it with the query string for proper error reporting.

2. **Relation Context**: Opens the target relation using the provided OID and adds it to the range table, enabling statistics expressions to reference table columns without qualification.

3. **Expression Processing**: For each statistics element that contains an expression:
   - Transforms the expression using the appropriate expression kind (EXPR_KIND_STATS_EXPRESSION)
   - Assigns proper collations to the transformed expressions
   - Ensures expressions are valid for statistics collection

4. **Validation**: Ensures that statistics expressions only reference the table being analyzed, preventing invalid cross-table references.

5. **Cleanup and Marking**: Properly cleans up the parse state, closes the relation, and marks the statement as transformed to prevent redundant processing.

The function includes the same race condition safety measures as transformIndexStmt, relying on the passed relid parameter rather than the statement's relation field. It also includes an early return optimization for already-transformed statements.

## Parameters / Member Variables
- : Object identifier of the relation for which statistics are being created
- : CreateStatsStmt structure containing the parsed statistics definition that needs transformation
- : Original SQL query string used for error reporting and parse state context

## Dependencies
- Functions called/Symbols referenced:
  - [make_parsestate](../m/make_parsestate.md)
  - [relation_open](../r/relation_open.md)
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md)
  - [addNSItemToQuery](../a/addNSItemToQuery.md)
  - [transformExpr](transformExpr.md)
  - [assign_expr_collations](../a/assign_expr_collations.md)
  - [free_parsestate](../f/free_parsestate.md)
  - [table_close](table_close.md)
- Called from (representative examples):
  - [ATPostAlterTypeParse](../A/ATPostAlterTypeParse.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Similar in structure to transformIndexStmt but specifically designed for statistics expressions
- The function prevents redundant processing through the transformed flag
- Race condition safety is ensured by using relid parameter instead of stmt->relation
- Statistics expressions are limited to referencing only the target table
- Proper collation assignment is essential for expression-based statistics
- The validation check for single table reference is noted as potentially dead code
- Used for extended statistics objects that can collect multi-column statistics or statistics on expressions

## Simplified Source

```c
CreateStatsStmt *
transformStatsStmt(Oid relid, CreateStatsStmt *stmt, const char *queryString)
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

    // Transform statistics expressions
    foreach(l, stmt->exprs) {
        StatsElem *selem = (StatsElem *) lfirst(l);

        if (selem->expr) {
            // Transform the expression for statistics use
            selem->expr = transformExpr(pstate, selem->expr, EXPR_KIND_STATS_EXPRESSION);

            // Fix collations for the expression
            assign_expr_collations(pstate, selem->expr);
        }
    }

    // Validate that only base relation is referenced
    if (list_length(pstate->p_rtable) != 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg("statistics expressions can refer only to the table being referenced")));

    // Clean up
    free_parsestate(pstate);
    table_close(rel, NoLock);

    // Mark as transformed to avoid reprocessing
    stmt->transformed = true;

    return stmt;
}
```