# transformIndexConstraints

## Location
src/backend/parser/parse_utilcmd.c: 2058 - 2160

## Overview
Handles UNIQUE, PRIMARY KEY, and EXCLUDE constraints that create indexes, merging in any index definitions from LIKE ... INCLUDING INDEXES clauses and removing redundant index specifications.

## Definition


## Detailed Description
The  function processes index-generating constraints during table creation or alteration. It transforms UNIQUE, PRIMARY KEY, and EXCLUDE constraints into corresponding IndexStmt nodes that will create the appropriate indexes. The function implements deduplication logic to remove redundant index specifications that might arise from overlapping constraints (e.g., when a column is marked both UNIQUE and PRIMARY KEY).

The function operates in two main phases:
1. **Constraint Processing**: Iterates through all index constraints in the context, calling  to convert each constraint into an IndexStmt
2. **Deduplication**: Removes redundant indexes by comparing index parameters, included parameters, WHERE clauses, exclude operators, access methods, and various index properties

Special handling ensures that PRIMARY KEY indexes are preserved in preference to other equivalent indexes, and named constraints transfer their names to previously unnamed equivalent indexes.

## Parameters / Member Variables
- : Pointer to CreateStmtContext containing the constraints to process and the target list for generated statements

## Dependencies
- Functions called/Symbols referenced:
  - transformIndexConstraint (converts individual constraints to IndexStmt)
  - equal (deep comparison of node structures)
  - list_concat (concatenates lists)
  - CreateStmtContext, IndexStmt, Constraint (data structures)
  - CONSTR_PRIMARY, CONSTR_UNIQUE, CONSTR_EXCLUSION (constraint type constants)
- Called from (representative examples):
  - transformCreateStmt (during CREATE TABLE processing)
  - transformAlterTableStmt (during ALTER TABLE processing)

## Notes and Other Information
- This is a static function in parse_utilcmd.c, part of the utility command parsing infrastructure
- Implements PostgreSQL's policy of allowing redundant constraints without error (e.g., UNIQUE PRIMARY KEY)
- The deduplication logic compares multiple index properties: parameters, included columns, WHERE clause, exclude operators, access method, nulls distinctness, deferrability
- PRIMARY KEY constraints receive special treatment and are always kept in the final index list
- Generated IndexStmt nodes are appended to the context's action list (cxt->alist) for later execution
- Supports both CREATE TABLE and ALTER TABLE scenarios through the same logic