# GetRTEByRangeTablePosn

## Location
src/backend/parser/parse_relation.c: 537 - 556

## Overview
Retrieves a Range Table Entry (RTE) from the parser state by its position in the range table, accounting for nested query contexts.

## Definition


## Detailed Description
This function locates and returns a specific Range Table Entry (RTE) from the PostgreSQL parser state structure. It navigates through potentially nested ParseState contexts using the sublevels_up parameter to find the appropriate query level, then retrieves the RTE at the specified position (varno) in that level's range table. The function is essential for resolving table references during query parsing, especially in complex queries with subqueries or CTEs where table references may need to be resolved at different nesting levels.

## Parameters / Member Variables
- : ParseState pointer representing the current parser state context
- : Integer position (1-based index) of the desired RTE in the range table
- : Number of parser state levels to traverse upward (0 means current level)

## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch
  - CommonTableExpr
- Called from (representative examples):
  - [count_rowexpr_columns](../c/count_rowexpr_columns.md)
  - [unknown_attribute](../u/unknown_attribute.md)
  - [markTargetListOrigin](../m/markTargetListOrigin.md)
  - [expandRecordVariable](../e/expandRecordVariable.md)

## Notes and Other Information
- The function includes assertions to ensure varno is within valid range (1 to length of range table)
- Uses 1-based indexing consistent with PostgreSQL's varno convention
- Critical for resolving table references in nested query contexts
- The RTE returned may not necessarily be in the current query's namespace
- Located in src/backend/parser/parse_relation.c:537-556