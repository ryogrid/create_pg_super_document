# createDummyViewAsClause

## Location
src/bin/pg_dump/pg_dump.c: 15906 - 15945

## Overview
Creates a dummy AS clause for a PostgreSQL view definition used when the real view definition must be postponed due to circular dependencies between database objects.

## Definition


## Detailed Description
This function generates a placeholder SELECT statement for a view that maintains the view's external properties (column names, types, and collations) while using NULL values for all columns. This is essential in pg_dump when circular dependencies prevent the immediate creation of a view with its actual definition. The dummy view preserves the schema structure so that dependent objects can reference it correctly, and it can later be replaced with the real view definition using CREATE OR REPLACE VIEW.

The function constructs a SELECT statement where each column is represented as "NULL::type_name COLLATE collation AS column_name", ensuring that the view interface remains consistent for dependent objects.

## Parameters / Member Variables
- : Archive structure containing dump context and configuration
- : TableInfo structure containing the view's metadata including column names, types, and collations

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - appendPQExpBufferStr  
  - appendPQExpBufferChar
  - appendPQExpBuffer
  - OidIsValid
  - findCollationByOid
  - fmtQualifiedDumpable
  - fmtId
- Types referenced:
  - Archive
  - TableInfo
  - CollInfo
  - PQExpBuffer
- Called from:
  - dumpTableSchema
  - dumpRule

## Notes and Other Information
- Returns a newly allocated PQExpBuffer that must be freed by the caller
- Handles collation specifications to ensure CREATE OR REPLACE VIEW operations preserve collations
- Only adds collation clauses for non-default collations to avoid redundancy
- Essential for resolving circular dependency issues in complex database schemas during pg_dump operations