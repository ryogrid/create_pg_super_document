# transformOptionalSelectInto

## Location
src/backend/parser/analyze.c: 272 - 310

## Overview
Converts SELECT statements with INTO clauses to CREATE TABLE AS statements, handling the transformation before entering recursive statement processing.

## Definition


## Detailed Description
This function performs a specialized transformation that converts SELECT ... INTO syntax into CREATE TABLE AS statements. It operates at the top level of the parse tree before entering the main recursive transformation process in transformStmt(). The function specifically handles:

1. Detection of SelectStmt nodes with INTO clauses
2. Navigation through set-operation trees to find the leftmost SelectStmt
3. Creation of CreateTableAsStmt nodes to replace SELECT INTO syntax
4. Removal of the original intoClause to prevent errors in subsequent processing

The transformation ensures that SELECT INTO statements are properly converted to the equivalent CREATE TABLE AS form, which is the internal representation used by PostgreSQL for table creation from query results.

## Parameters / Member Variables
- : ParseState context containing parsing state information and error reporting context
- : Node representing the top-level statement to be potentially transformed

## Dependencies
- Functions called/Symbols referenced:
  - SelectStmt (structure access and casting)
  - SETOP_NONE (constant for detecting non-set operations)
  - CreateTableAsStmt (node creation via makeNode)
  - OBJECT_TABLE (constant for object type specification)
  - transformStmt (recursive statement transformation)

- Called from (representative examples):
  - transformTopLevelStmt (main entry point for top-level statement transformation)
  - transformExplainStmt (for EXPLAIN statement processing)

## Notes and Other Information
- This transformation only occurs at the top level of the parse tree since utility statements cannot be nested within other statements
- The function safely handles set-operation trees by drilling down to the leftmost SelectStmt to find the INTO clause
- After transformation, the original intoClause is removed to ensure transformSelectStmt can properly validate that INTO clauses don't appear in disallowed contexts
- The is_select_into flag is set to true in the created CreateTableAsStmt to distinguish this case from explicit CREATE TABLE AS statements