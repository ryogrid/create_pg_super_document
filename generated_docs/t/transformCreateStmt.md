# transformCreateStmt

## Location
[src/backend/parser/parse_utilcmd.c:163-360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L163-L360)

## Overview
Performs parse analysis for CREATE TABLE statements, transforming the raw parser output into a list of utility commands to be executed in sequence to create the table and its associated objects.

## Definition

```c
List *
transformCreateStmt(CreateStmt *stmt, const char *queryString)
```
## Detailed Description
transformCreateStmt is the main entry point for processing CREATE TABLE statements in PostgreSQL's parser. It takes a raw CreateStmt from the parser and transforms it into a comprehensive list of utility commands that will be executed to create the table. The function handles the complete lifecycle of table creation including:

- Namespace resolution and permission checking
- Handling IF NOT EXISTS logic with duplicate relation detection
- Processing column definitions, constraints, and table-like clauses
- Generating additional statements for indexes, foreign keys, and check constraints
- Organizing the execution order to ensure dependencies are satisfied

The function separates different types of table elements (columns, constraints, LIKE clauses) and processes them in the correct order. It ensures that primary keys are created before foreign keys, and that all constraints are properly validated. For foreign tables, certain validations are skipped since the data resides externally.

## Parameters / Member Variables
- : The CreateStmt node from the parser containing the raw CREATE TABLE specification
- : The original SQL query string for error reporting and context

## Dependencies
- Functions called/Symbols referenced:
  - [make_parsestate](../m/make_parsestate.md)
  - [RangeVarGetAndCheckCreationNamespace](../R/RangeVarGetAndCheckCreationNamespace.md)
  - [checkMembershipInCurrentExtension](../c/checkMembershipInCurrentExtension.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [transformOfType](transformOfType.md)
  - [transformColumnDefinition](transformColumnDefinition.md)
  - [transformTableConstraint](transformTableConstraint.md)
  - [transformTableLikeClause](transformTableLikeClause.md)
  - [transformIndexConstraints](transformIndexConstraints.md)
  - [transformFKConstraints](transformFKConstraints.md)
  - [transformCheckConstraints](transformCheckConstraints.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
The function maintains a strict execution order for different types of constraints and table elements. LIKE clauses are processed after index creation but before foreign key creation to handle cases where LIKE clauses generate primary keys that might conflict with explicitly defined primary keys. The function returns a list that may contain the original CreateStmt plus additional utility statements like AlterTableStmt and IndexStmt that need to be executed to fully create the table and its associated objects.

## Simplified Source

```c
List *
transformCreateStmt(CreateStmt *stmt, const char *queryString)
{
    ParseState *pstate;
    CreateStmtContext cxt;
    List *result;
    Oid namespaceid;
    Oid existing_relid;

    // Set up parse state for error reporting
    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;

    // Look up creation namespace and check permissions
    namespaceid = RangeVarGetAndCheckCreationNamespace(stmt->relation, NoLock, &existing_relid);

    // Handle IF NOT EXISTS case - skip if relation already exists
    if (stmt->if_not_exists && OidIsValid(existing_relid)) {
        // Security check for extensions
        checkMembershipInCurrentExtension(&address);

        // Report notice and return empty list
        ereport(NOTICE, (errcode(ERRCODE_DUPLICATE_TABLE),
                errmsg("relation \"%s\" already exists, skipping", stmt->relation->relname)));
        return NIL;
    }

    // Qualify relation name if needed (except temp tables)
    if (stmt->relation->schemaname == NULL && stmt->relation->relpersistence != RELPERSISTENCE_TEMP)
        stmt->relation->schemaname = get_namespace_name(namespaceid);

    // Initialize transformation context
    cxt.pstate = pstate;
    cxt.stmtType = IsA(stmt, CreateForeignTableStmt) ? "CREATE FOREIGN TABLE" : "CREATE TABLE";
    cxt.isforeign = IsA(stmt, CreateForeignTableStmt);
    cxt.relation = stmt->relation;
    cxt.ispartitioned = (stmt->partspec != NULL);
    // ... initialize other context fields to NIL/NULL

    // Handle OF TYPE clause if present
    if (stmt->ofTypename)
        transformOfType(&cxt, stmt->ofTypename);

    // Process each table element (columns, constraints, LIKE clauses)
    foreach(elements, stmt->tableElts) {
        Node *element = lfirst(elements);

        switch (nodeTag(element)) {
            case T_ColumnDef:
                transformColumnDefinition(&cxt, (ColumnDef *) element);
                break;
            case T_Constraint:
                transformTableConstraint(&cxt, (Constraint *) element);
                break;
            case T_TableLikeClause:
                transformTableLikeClause(&cxt, (TableLikeClause *) element);
                break;
            default:
                elog(ERROR, "unrecognized node type: %d", (int) nodeTag(element));
        }
    }

    // Process constraints in specific order:
    // 1. Index constraints (PRIMARY KEY, UNIQUE)
    transformIndexConstraints(&cxt);

    // 2. LIKE clauses (after indexes, before foreign keys)
    cxt.alist = list_concat(cxt.alist, cxt.likeclauses);

    // 3. Foreign key constraints
    transformFKConstraints(&cxt, true, false);

    // 4. Check constraints (marked valid for regular tables, not foreign)
    transformCheckConstraints(&cxt, !cxt.isforeign);

    // Prepare final output
    stmt->tableElts = cxt.columns;
    stmt->constraints = cxt.ckconstraints;

    // Build result list: before-statements + main statement + after-statements
    result = lappend(cxt.blist, stmt);
    result = list_concat(result, cxt.alist);

    return result;
}
```