# expandTableLikeClause

## Location
src/backend/parser/parse_utilcmd.c: 1169 - 1460

## Overview
Processes LIKE clause options that require knowing the final column assignments in a newly created table, generating utility commands for indexes, constraints, defaults, and statistics after table creation.

## Definition
List *expandTableLikeClause(RangeVar *heapRel, TableLikeClause *table_like_clause)

## Detailed Description
This function executes after DefineRelation has been called for a new table and handles the post-creation processing of TABLE LIKE clauses. It analyzes the source table specified in the LIKE clause and generates a list of utility commands (ALTER TABLE, CREATE INDEX, CREATE STATISTICS, COMMENT) needed to replicate the requested features from the source table to the newly created table. The function maps attribute numbers between source and target tables and handles defaults, check constraints, indexes, extended statistics, and comments based on the specified LIKE options.

## Parameters / Member Variables
- `heapRel`: RangeVar specifying the newly created table that should receive the LIKE clause features
- `table_like_clause`: TableLikeClause containing the source table OID and option flags specifying which features to copy

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md)
  - [relation_openrv](../r/relation_openrv.md)  
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md)
  - [TupleDescGetDefault](../T/TupleDescGetDefault.md)
  - [map_variable_attnos](../m/map_variable_attnos.md)
  - [stringToNode](../s/stringToNode.md)
  - [nodeToString](../n/nodeToString.md)
  - [GetComment](../G/GetComment.md)
  - [get_relation_constraint_oid](../g/get_relation_constraint_oid.md)
  - [generateClonedIndexStmt](../g/generateClonedIndexStmt.md)
  - [generateClonedExtStatsStmt](../g/generateClonedExtStatsStmt.md)
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [RelationGetStatExtList](../R/RelationGetStatExtList.md)
  - [index_open](../i/index_open.md)
  - [index_close](../i/index_close.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Requires that transformTableLikeClause has already been called to validate and lock the source table
- Uses attribute mapping to ensure proper column correspondence between source and target tables
- Rejects whole-row table references in constraints and defaults to prevent future incompatibilities
- Generates commands in specific order: ALTER TABLE first, then indexes/statistics, then comments
- Maintains locks on both source and target tables throughout the operation
- Supports selective copying via option flags: DEFAULTS, GENERATED, CONSTRAINTS, INDEXES, STATISTICS, COMMENTS