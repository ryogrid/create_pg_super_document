# transformCreateTableAsStmt

## Location
src/backend/parser/analyze.c: 3013 - 3087

## Overview
Transforms CREATE TABLE AS, SELECT INTO, or CREATE MATERIALIZED VIEW statements into CMD_UTILITY Query nodes with additional validation for materialized views.

## Definition


## Detailed Description
This function transforms statements that create new tables or materialized views from the results of a query. It handles three types of statements: CREATE TABLE AS, SELECT INTO, and CREATE MATERIALIZED VIEW. The function first transforms the contained query, then applies additional validation and processing specific to materialized views.

For materialized views, the function performs several important checks:
1. Prohibits data-modifying CTEs in the defining query
2. Prevents use of temporary database objects that could disappear
3. Disallows bound parameters since they complicate maintenance/refresh operations
4. Rejects unlogged materialized views due to crash recovery concerns

The function also preserves a copy of the parsed query for materialized views, which is needed at runtime to create the view's ON SELECT rule.

## Parameters / Member Variables
- : Parse state containing context information for the transformation
- : The CREATE TABLE AS statement to transform, containing:
  - : The SELECT query that provides the data/structure
  - : IntoClause specifying the target table/view details
  - : Object type (table, materialized view, etc.)
  - : Relation kind for the created object

## Dependencies
- Functions called/Symbols referenced:
  - [transformStmt](transformStmt.md), makeNode, copyObject
  - [isQueryUsingTempRelation](../i/isQueryUsingTempRelation.md), query_contains_extern_params
  - ereport, errcode, errmsg
- Constants referenced:
  - OBJECT_MATVIEW, RELPERSISTENCE_UNLOGGED, CMD_UTILITY
  - Error codes: ERRCODE_FEATURE_NOT_SUPPORTED
- Called from (representative examples):
  - [transformStmt](transformStmt.md)

## Notes and Other Information
- Unlike SELECT statements, this transformation prohibits nested SELECT INTO
- Materialized views have stricter validation than regular CREATE TABLE AS
- The function stores a copy of the transformed query in the IntoClause for materialized views
- Unlogged materialized views are prohibited due to crash recovery limitations
- Temporary objects are disallowed in materialized view definitions for refresh reliability
- The preserved query copy is used by intorel_startup() to create the view's ON SELECT rule
- Bound parameters are forbidden in materialized views since they would complicate refresh operations