# transformIndexStmt

## Location
src/backend/parser/parse_utilcmd.c: 2797 - 2891

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
  - make_parsestate
  - relation_open
  - addRangeTableEntryForRelation
  - addNSItemToQuery
  - transformWhereClause
  - assign_expr_collations
  - FigureIndexColname
  - transformExpr
  - free_parsestate
  - table_close
- Called from (representative examples):
  - ATPostAlterTypeParse
  - transformAlterTableStmt
  - ProcessUtilitySlow

## Notes and Other Information
- The function is a no-op for simple indexes that don't use expressions or predicates
- Several code paths create indexes without calling this function when they know no expressions need processing
- The transformed flag prevents redundant processing of already-transformed statements
- Race condition safety is achieved by using the relid parameter instead of stmt->relation
- The function validates that expressions only reference the target table, though this check is noted as potentially dead code
- Proper collation handling is crucial for both WHERE clauses and index expressions